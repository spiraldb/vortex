use vortex::match_each_native_ptype;
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::compute::{ArrayCompute, ScalarAtFn};
use crate::primitive::PrimitiveArray;
use crate::ArrayValidity;

impl ArrayCompute for &dyn PrimitiveArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for &dyn PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            match_each_native_ptype!(self.ptype(), |$T| {
                Scalar::from(self.buffer().typed_data::<$T>()[index]).cast(self.dtype())
            })
        } else {
            Ok(Scalar::null(self.dtype()))
        }
    }
}
