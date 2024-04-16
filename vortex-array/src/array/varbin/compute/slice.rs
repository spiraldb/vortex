use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::compute::slice::{slice, SliceFn};
use crate::{ArrayDType, IntoArray, OwnedArray};

impl SliceFn for VarBinArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        Ok(VarBinArray::new(
            slice(&self.offsets(), start, stop + 1)?,
            self.bytes().clone(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )
        .into_array())
    }
}
