use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::chunked::ChunkedArray;
use crate::compute::unary::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::unary::scalar_subtract::SubtractScalarFn;
use crate::compute::{ArrayCompute, SliceFn, TakeFn};

mod slice;
mod take;

impl ArrayCompute for ChunkedArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn subtract_scalar(&self) -> Option<&dyn SubtractScalarFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for ChunkedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let (chunk_index, chunk_offset) = self.find_chunk_idx(index);
        scalar_at(&self.chunk(chunk_index).unwrap(), chunk_offset)
    }
}
