use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::{Array, ArrayDType, IntoArray, OwnedArray};
use vortex_error::VortexResult;

use crate::DateTimePartsArray;

impl ArrayCompute for DateTimePartsArray<'_> {
    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl TakeFn for DateTimePartsArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        Ok(DateTimePartsArray::try_new(
            self.dtype().clone(),
            take(&self.days(), indices)?,
            take(&self.seconds(), indices)?,
            take(&self.subsecond(), indices)?,
        )?
        .into_array())
    }
}

impl SliceFn for DateTimePartsArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        Ok(DateTimePartsArray::try_new(
            self.dtype().clone(),
            slice(&self.days(), start, stop)?,
            slice(&self.seconds(), start, stop)?,
            slice(&self.subsecond(), start, stop)?,
        )?
        .into_array())
    }
}
