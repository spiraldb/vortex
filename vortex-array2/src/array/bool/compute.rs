use vortex::scalar::{BoolScalar, Scalar};
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::compute::{ArrayCompute, ScalarAtFn};
use crate::validity::ArrayValidity;
use crate::ArrayTrait;

impl ArrayCompute for BoolArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for BoolArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            let value = self.boolean_buffer().value(index);
            Ok(Scalar::Bool(
                BoolScalar::try_new(Some(value), self.dtype().nullability()).unwrap(),
            ))
        } else {
            Ok(Scalar::null(self.dtype()))
        }
    }
}
