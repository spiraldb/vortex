use crate::array::{Array, ArrayRef};
use crate::error::{EncError, EncResult};

pub trait TakeFn {
    fn take(&self, indices: &dyn Array) -> EncResult<ArrayRef>;
}

pub fn take(array: &dyn Array, indices: &dyn Array) -> EncResult<ArrayRef> {
    array
        .compute()
        .and_then(|c| c.take())
        .map(|t| t.take(indices))
        .unwrap_or_else(|| {
            // TODO(ngates): default implementation of decode and then try again
            Err(EncError::ComputeError(
                format!("take not implemented for {}", &array.encoding().id()).into(),
            ))
        })
}
