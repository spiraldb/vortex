use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::bool::BoolArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::validity::ArrayValidity;
use crate::ArrayDType;

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            let value = self.boolean_buffer().value(index);
            let s = Scalar::new(
                self.dtype().clone(),
                vortex_scalar::ScalarValue::Bool(value),
            );
            Ok(s)
        } else {
            return Ok(Scalar::null(self.dtype().clone()));
        }
    }
}
