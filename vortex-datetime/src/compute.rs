use crate::DateTimeArray;
use vortex::array::{Array, ArrayRef};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::validity::ArrayValidity;
use vortex_error::VortexResult;

impl ArrayCompute for DateTimeArray {
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
            self.validity(),
            self.dtype().clone(),
        )
        .into_array())
    }
}
