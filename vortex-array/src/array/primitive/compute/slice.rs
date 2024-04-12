use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::slice::SliceFn;
use crate::ptype::NativePType;

impl<T: NativePType> SliceFn for &dyn PrimitiveTrait<T> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        let byte_start = start * self.ptype().byte_width();
        let byte_length = (stop - start) * self.ptype().byte_width();

        Ok(PrimitiveArray::new(
            self.ptype(),
            self.buffer().slice_with_length(byte_start, byte_length),
            self.validity().map(|v| v.slice(start, stop)).transpose()?,
        )
        .into_array())
    }
}
