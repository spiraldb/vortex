use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::Array;
use crate::compute::scalar_at::ScalarAtFn;
use crate::ptype::NativePType;
use crate::scalar::{PrimitiveScalar, Scalar};

impl<T: NativePType> ScalarAtFn for &dyn PrimitiveTrait<T> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(PrimitiveScalar::try_new(
            self.validity_view()
                .map(|v| v.is_valid(index))
                .unwrap_or(true)
                .then(|| self.typed_data()[index]),
            self.dtype().nullability(),
        )?
        .into())
    }
}
