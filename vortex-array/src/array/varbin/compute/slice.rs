use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef};
use crate::compute::slice::{slice, SliceFn};
use crate::validity::OwnedValidity;

impl SliceFn for VarBinArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(VarBinArray::new(
            slice(self.offsets(), start, stop + 1)?,
            self.bytes().clone(),
            self.dtype().clone(),
            self.validity().map(|v| v.slice(start, stop)).transpose()?,
        )
        .into_array())
    }
}
