use vortex::compute::{filter, FilterFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;

use crate::ALPRDArray;

impl FilterFn for ALPRDArray {
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        let left_parts_exceptions = match self.left_parts_exceptions() {
            None => None,
            Some(exc) => Some(filter(&exc, predicate)?),
        };

        Ok(ALPRDArray::try_new(
            self.dtype().clone(),
            filter(self.left_parts(), predicate)?,
            self.left_parts_dict(),
            filter(self.right_parts(), predicate)?,
            self.right_bit_width(),
            left_parts_exceptions,
        )?
        .into_array())
    }
}
