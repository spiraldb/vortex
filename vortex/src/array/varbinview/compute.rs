use crate::array::varbinview::VarBinViewArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar, ScalarRef};

impl ArrayCompute for VarBinViewArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinViewArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
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
