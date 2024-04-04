use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::array::ArrayValidity;
use crate::compute::scalar_at::ScalarAtFn;
use crate::match_each_native_ptype;
use crate::scalar::{PrimitiveScalar, Scalar};

impl ScalarAtFn for PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match_each_native_ptype!(self.ptype, |$T| {
            Ok(PrimitiveScalar::try_new(
                self.is_valid(index).then(|| self.typed_data::<$T>()[index]),
                self.nullability(),
            )?.into())
        })
    }
}
