use vortex::match_each_native_ptype;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::slice::SliceFn;
use crate::IntoArray;
use crate::OwnedArray;

impl SliceFn for PrimitiveArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        match_each_native_ptype!(self.ptype(), |$T| {
            Ok(PrimitiveArray::try_new(
                self.scalar_buffer::<$T>().slice(start, stop - start),
                self.validity().slice(start, stop)?,
            )?
            .into_array())
        })
    }
}
