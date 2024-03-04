use crate::array::bool::BoolArray;
use crate::array::Array;
use crate::compute::cast::CastBoolFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar, ScalarRef};

impl ArrayCompute for BoolArray {
    fn cast_bool(&self) -> Option<&dyn CastBoolFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl CastBoolFn for BoolArray {
    fn cast_bool(&self) -> VortexResult<BoolArray> {
        Ok(self.clone())
    }
}

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
        if self.is_valid(index) {
            Ok(self.buffer.value(index).into())
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }
}
