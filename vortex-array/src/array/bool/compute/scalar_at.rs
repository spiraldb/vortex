use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::BoolArray;
use crate::compute::unary::ScalarAtFn;

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar_at_unchecked(index))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        self.boolean_buffer().value(index).into()
    }
}
