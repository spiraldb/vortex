use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::match_each_native_ptype;
use crate::scalar::{NullableScalar, Scalar};

impl ArrayCompute for PrimitiveArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        if self.is_valid(index) {
            Ok(
                match_each_native_ptype!(self.ptype, |$T| self.buffer.typed_data::<$T>()
                    .get(index)
                    .unwrap()
                    .clone()
                    .into()
                ),
            )
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }
}
