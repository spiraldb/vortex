use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::SliceFn;
use crate::{Array, IntoArray};

impl SliceFn for PrimitiveArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let byte_width = self.ptype().byte_width();
        let buffer = self.buffer().slice(start * byte_width..stop * byte_width);
        Ok(
            PrimitiveArray::new(buffer, self.ptype(), self.validity().slice(start, stop)?)
                .into_array(),
        )
    }
}
