use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::chunked::ChunkedArray;
use crate::compute::unary::{scalar_at, try_cast, CastFn, ScalarAtFn, SubtractScalarFn};
use crate::compute::{ArrayCompute, SliceFn, TakeFn};
use crate::{Array, IntoArray};

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

impl CastFn for ChunkedArray {
    fn cast(&self, dtype: &DType) -> VortexResult<Array> {
        let mut cast_chunks = Vec::new();
        for chunk in self.chunks() {
            cast_chunks.push(try_cast(&chunk, dtype)?);
        }

        Ok(ChunkedArray::try_new(cast_chunks, dtype.clone())?.into_array())
    }
}
