use vortex_dtype::PType;
use vortex_error::VortexResult;

use crate::array::chunked::ChunkedArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::take::{take, TakeFn};
use crate::compute::unary::cast::try_cast;
use crate::ArrayDType;
use crate::{Array, IntoArray, ToArray};

impl TakeFn for ChunkedArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        if self.len() == indices.len() {
            return Ok(self.to_array());
        }

        let indices = PrimitiveArray::try_from(try_cast(indices, PType::U64.into())?)?;

        // While the chunk idx remains the same, accumulate a list of chunk indices.
        let mut chunks = Vec::new();
        let mut indices_in_chunk = Vec::new();
        let mut prev_chunk_idx = self
            .find_chunk_idx(indices.maybe_null_slice::<u64>()[0] as usize)
            .0;
        for idx in indices.maybe_null_slice::<u64>() {
            let (chunk_idx, idx_in_chunk) = self.find_chunk_idx(*idx as usize);

            if chunk_idx != prev_chunk_idx {
                // Start a new chunk
                let indices_in_chunk_array = indices_in_chunk.clone().into_array();
                chunks.push(take(
                    &self.chunk(prev_chunk_idx).unwrap(),
                    &indices_in_chunk_array,
                )?);
                indices_in_chunk = Vec::new();
            }

            indices_in_chunk.push(idx_in_chunk as u64);
            prev_chunk_idx = chunk_idx;
        }

        if !indices_in_chunk.is_empty() {
            let indices_in_chunk_array = indices_in_chunk.into_array();
            chunks.push(take(
                &self.chunk(prev_chunk_idx).unwrap(),
                &indices_in_chunk_array,
            )?);
        }

        Ok(Self::try_new(chunks, self.dtype().clone())?.into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::chunked::ChunkedArray;
    use crate::compute::take::take;
    use crate::{ArrayDType, ArrayTrait, AsArray, IntoArray, IntoCanonical};

    #[test]
    fn test_take() {
        let a = vec![1i32, 2, 3].into_array();
        let arr = ChunkedArray::try_new(vec![a.clone(), a.clone(), a.clone()], a.dtype().clone())
            .unwrap();
        assert_eq!(arr.nchunks(), 3);
        assert_eq!(arr.len(), 9);
        let indices = vec![0, 0, 6, 4].into_array();

        let result = &ChunkedArray::try_from(take(arr.as_array_ref(), &indices).unwrap())
            .unwrap()
            .into_array()
            .into_canonical()
            .unwrap()
            .into_primitive()
            .unwrap();
        assert_eq!(result.maybe_null_slice::<i32>(), &[1, 1, 1, 2]);
    }
}
