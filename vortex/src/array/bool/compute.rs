use crate::array::bool::BoolArray;
use crate::array::Array;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar};

impl ArrayCompute for BoolArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        if self.is_valid(index) {
            Ok(self.buffer.value(index).into())
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }
}
