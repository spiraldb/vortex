use vortex::array::{Array, ArrayRef};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::validity::OwnedValidity;
use vortex::view::ToOwnedView;
use vortex_error::VortexResult;

use crate::DateTimeArray;

impl ArrayCompute for DateTimeArray {
    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl TakeFn for DateTimeArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(DateTimeArray::new(
            take(self.days(), indices)?,
            take(self.seconds(), indices)?,
            take(self.subsecond(), indices)?,
            self.validity().to_owned_view(),
            self.dtype().clone(),
        )
        .into_array())
    }
}

impl SliceFn for DateTimeArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(DateTimeArray::new(
            slice(self.days(), start, stop)?,
            slice(self.seconds(), start, stop)?,
            slice(self.subsecond(), start, stop)?,
            self.validity().map(|v| v.slice(start, stop)).transpose()?,
            self.dtype().clone(),
        )
        .into_array())
    }
}
