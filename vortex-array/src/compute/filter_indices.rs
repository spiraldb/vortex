use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_expr::expressions::{Conjunction, Disjunction, Predicate};

use crate::{Array, ArrayDType};

pub trait FilterIndicesFn {
    fn apply_disjunctive_filter(&self, predicate: &Disjunction) -> VortexResult<Array>;
    fn apply_conjunctive_filter(&self, predicate: &Conjunction) -> VortexResult<Array>;
    fn indices_matching_predicate(&self, predicate: &Predicate) -> VortexResult<Vec<bool>>;
}

pub fn filter_indices(array: &Array, predicate: &Conjunction) -> VortexResult<Array> {
    if let Some(subtraction_result) =
        array.with_dyn(|c| c.filter_indices().map(|t| t.apply_conjunctive_filter(predicate)))
    {
        return subtraction_result;
    }
    // if filter is not implemented for the given array type, but the array has a numeric
    // DType, we can flatten the array and apply filter to the flattened primitive array
    match array.dtype() {
        DType::Primitive(..) => {
            let flat = array.clone().flatten_primitive()?;
            flat.apply_conjunctive_filter(predicate)
        }
        _ => Err(vortex_err!(
            NotImplemented: "filter_indices",
            array.encoding().id()
        )),
    }
}
