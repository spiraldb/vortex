use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::BoolArray;
use crate::compute::unary::ScalarAtFn;
use crate::ArrayDType;

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar_at_unchecked(index))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        // SAFETY:
        // `scalar_at_unchecked` is fine with undefined behavior, so it should be acceptable here
        unsafe {
            Scalar::bool(
                self.boolean_buffer().value_unchecked(index),
                self.dtype().nullability(),
            )
        }
    }
}
