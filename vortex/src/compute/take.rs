use crate::array::{Array, ArrayRef};
use crate::error::{VortexError, VortexResult};

pub trait TakeFn {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef>;
}

pub fn take(array: &dyn Array, indices: &dyn Array) -> VortexResult<ArrayRef> {
    array
        .compute()
        .and_then(|c| c.take())
        .map(|t| t.take(indices))
        .unwrap_or_else(|| {
            // TODO(ngates): default implementation of decode and then try again
            Err(VortexError::ComputeError(
                format!("take not implemented for {}", &array.encoding().id()).into(),
            ))
        })
}
