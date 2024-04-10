use vortex::match_each_native_ptype;
use vortex::scalar::PrimitiveScalar;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::validity::ArrayValidity;

impl ScalarAtFn for PrimitiveArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        return Ok(PrimitiveScalar::try_new(
            self.is_valid(index)
                .then(|| self.typed_data::<u16>()[index]),
            self.nullability(),
        )?
        .into());

        match_each_native_ptype!(self.ptype, |$T| {
            Ok(PrimitiveScalar::try_new(
                self.is_valid(index).then(|| self.typed_data::<$T>()[index]),
                self.nullability(),
            )?.into())
        })
    }
}
