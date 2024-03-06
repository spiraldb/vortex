use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::compute::scalar_at::{ScalarAtFn, UsizeAtFn};
use crate::error::{VortexError, VortexResult};
use crate::match_each_native_ptype;
use crate::scalar::{NullableScalar, Scalar, ScalarRef};
use num_traits::AsPrimitive;

impl ScalarAtFn for PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
        if self.is_valid(index) {
            Ok(match_each_native_ptype!(self.ptype, |$T| self.typed_data::<$T>()[index].into()))
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }
}

impl UsizeAtFn for PrimitiveArray {
    fn usize_at(&self, index: usize) -> VortexResult<usize> {
        if self.is_valid(index) {
            Ok(match_each_native_ptype!(self.ptype, |$T| self.typed_data::<$T>()[index].as_()))
        } else {
            Err(VortexError::ComputeError("Value is null".into()))
        }
    }
}
