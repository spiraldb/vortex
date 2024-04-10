use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray};
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::validity::ArrayValidity;

impl AsArrowArray for BoolArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        Ok(Arc::new(ArrowBoolArray::new(
            self.buffer().clone(),
            self.logical_validity().to_null_buffer()?,
        )))
    }
}
