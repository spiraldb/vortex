use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::compute::{slice, SliceFn};
use crate::{Array, ArrayDType, IntoArray};

impl SliceFn for VarBinArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Self::try_new(
            slice(self.offsets(), start, stop + 1)?,
            self.bytes(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )
        .map(|a| a.into_array())
    }
}
