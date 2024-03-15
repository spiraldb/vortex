use crate::array::typed::TypedArray;
use crate::array::Array;
use crate::compute::as_arrow::AsArrowArray;
use crate::dtype::DType;
use crate::error::{VortexError, VortexResult};
use arrow_array::ArrayRef as ArrowArrayRef;

impl AsArrowArray for TypedArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // Decide based on the DType if we know how to do this or not...
        match self.dtype() {
            DType::Composite(id, _storage_dtype, _metadata) => match id.as_str() {
                &_ => Err(VortexError::InvalidArgument(
                    format!("Cannot convert composite DType {} to arrow", id).into(),
                )),
            },
            _ => Err(VortexError::InvalidDType(self.dtype().clone())),
        }
    }
}
