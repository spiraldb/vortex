use vortex::array::{Array, ArrayRef};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::validity::OwnedValidity;
use vortex::view::ToOwnedView;
use vortex_error::VortexResult;

use crate::DateTimeArray;

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
            self.validity().to_owned_view(),
            self.dtype().clone(),
        )
        .into_array())
    }
}
