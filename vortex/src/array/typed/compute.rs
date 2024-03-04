use crate::array::typed::TypedArray;
use crate::array::Array;
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::Scalar;

impl ArrayCompute for TypedArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for TypedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        let underlying = scalar_at(self.array.as_ref(), index)?;
        underlying.as_ref().cast(self.dtype())
    }
}
