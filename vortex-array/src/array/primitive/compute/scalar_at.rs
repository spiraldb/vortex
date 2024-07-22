use vortex_dtype::match_each_native_ptype;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::compute::unary::ScalarAtFn;
use crate::validity::ArrayValidity;
use crate::ArrayDType;

impl ScalarAtFn for PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match_each_native_ptype!(self.ptype(), |$T| {
            if self.is_valid(index) {
                Ok(Scalar::primitive(self.maybe_null_slice::<$T>()[index], self.dtype().nullability()))
            } else {
                Ok(Scalar::null(self.dtype().clone()))
            }
        })
    }
}
