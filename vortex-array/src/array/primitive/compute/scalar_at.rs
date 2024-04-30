use vortex_dtype::match_each_native_ptype;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::scalar::PrimitiveScalar;
use crate::scalar::Scalar;
use crate::validity::ArrayValidity;
use crate::ArrayDType;

impl ScalarAtFn for PrimitiveArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match_each_native_ptype!(self.ptype(), |$T| {
            Ok(PrimitiveScalar::try_new(
                self.is_valid(index)
                    .then(|| self.typed_data::<$T>()[index]),
                self.dtype().nullability(),
            )?
            .into())
        })
    }
}
