use crate::array::varbin::VarBinArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar};

impl ArrayCompute for VarBinArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        if self.is_valid(index) {
            self.bytes_at(index).map(|bytes| {
                if matches!(self.dtype, DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(bytes) }.into()
                } else {
                    bytes.into()
                }
            })
        } else {
            Ok(NullableScalar::none(self.dtype.clone()).boxed())
        }
    }
}
