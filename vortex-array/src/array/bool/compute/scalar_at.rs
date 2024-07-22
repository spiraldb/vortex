use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::bool::BoolArray;
use crate::compute::unary::ScalarAtFn;
use crate::validity::ArrayValidity;
use crate::ArrayDType;

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            Ok(self.boolean_buffer().value(index).into())
        } else {
            return Ok(Scalar::null(self.dtype().clone()));
        }
    }
}
