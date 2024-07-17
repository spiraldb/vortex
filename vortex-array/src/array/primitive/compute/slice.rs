use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::SliceFn;
use crate::Array;
use crate::IntoArray;

impl SliceFn for PrimitiveArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        assert!(start <= stop, "start must be <= stop");
        let byte_width = self.ptype().byte_width();
        let buffer = self.buffer().slice(start * byte_width..stop * byte_width);
        Ok(
            PrimitiveArray::new(buffer, self.ptype(), self.validity().slice(start, stop)?)
                .into_array(),
        )
    }
}
