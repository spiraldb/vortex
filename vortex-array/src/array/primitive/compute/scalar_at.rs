use vortex_dtype::match_each_native_ptype;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::validity::ArrayValidity;
use crate::ArrayDType;

impl ScalarAtFn for PrimitiveArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match_each_native_ptype!(self.ptype(), |$T| {
            if self.is_valid(index) {
                Ok(Scalar::primitive(self.typed_data::<$T>()[index], self.dtype().nullability()))
            } else {
                Ok(Scalar::null(self.dtype().clone()))
            }
        })
    }
}
