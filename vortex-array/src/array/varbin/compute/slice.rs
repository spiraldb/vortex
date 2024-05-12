use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::compute::slice::{slice, SliceFn};
use crate::{ArrayDType, IntoArray, OwnedArray};

impl SliceFn for VarBinArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        VarBinArray::try_new(
            slice(&self.offsets(), start, stop + 1)?,
            self.bytes().clone(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )
        .map(|a| a.into_array())
    }
}
