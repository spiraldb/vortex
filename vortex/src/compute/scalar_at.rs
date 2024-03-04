use crate::array::Array;
use crate::error::{VortexError, VortexResult};
use crate::scalar::Scalar;

pub trait ScalarAtFn {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>>;
}

pub fn scalar_at(array: &dyn Array, index: usize) -> VortexResult<Box<dyn Scalar>> {
    if index >= array.len() {
        return Err(VortexError::OutOfBounds(index, 0, array.len()));
    }

    array
        .compute()
        .and_then(|c| c.scalar_at())
        .map(|t| t.scalar_at(index))
        .unwrap_or_else(|| {
            // TODO(ngates): default implementation of decode and then try again
            Err(VortexError::ComputeError(
                format!("scalar_at not implemented for {}", &array.encoding().id()).into(),
            ))
        })
}
