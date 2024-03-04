use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::Array;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar};

impl ArrayCompute for ChunkedArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for ChunkedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        let (chunk_index, chunk_offset) = self.find_physical_location(index);
        self.chunks[chunk_index].scalar_at(chunk_offset)
    }
}
