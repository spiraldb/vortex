use itertools::Itertools;
use vortex_dtype::PType;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::chunked::ChunkedArray;
use crate::array::primitive::PrimitiveArray;
use crate::compute::unary::cast::try_cast;
use crate::compute::unary::scalar_at::scalar_at;
use crate::compute::unary::scalar_subtract::subtract_scalar;
use crate::compute::{search_sorted, slice, take, SearchSortedSide, TakeFn};
use crate::stats::ArrayStatistics;
use crate::{Array, ArrayDType, IntoArray, ToArray};

impl TakeFn for ChunkedArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        // Fast path for strict sorted indices.
        if indices
            .statistics()
            .compute_is_strict_sorted()
            .unwrap_or(false)
        {
            if self.len() == indices.len() {
                return Ok(self.to_array());
            }

            return take_strict_sorted(self, indices);
        }

        // FIXME(ngates): this is wrong, need to canonicalise
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

/// When the indices are non-null and strict-sorted, we can do better
fn take_strict_sorted(chunked: &ChunkedArray, indices: &Array) -> VortexResult<Array> {
    let mut indices_by_chunk = vec![None; chunked.nchunks()];

    // Track our position in the indices array
    let mut pos = 0;
    while pos < indices.len() {
        // Locate the chunk index for the current index
        let idx = usize::try_from(&scalar_at(indices, pos)?).unwrap();
        let (chunk_idx, _idx_in_chunk) = chunked.find_chunk_idx(idx);

        // Find the end of this chunk, and locate that position in the indices array.
        let chunk_begin = usize::try_from(&scalar_at(&chunked.chunk_ends(), chunk_idx)?).unwrap();
        let chunk_end = usize::try_from(&scalar_at(&chunked.chunk_ends(), chunk_idx + 1)?).unwrap();
        let chunk_end_pos = search_sorted(indices, chunk_end, SearchSortedSide::Left)
            .unwrap()
            .to_index();

        // Now we can say the slice of indices belonging to this chunk is [pos, chunk_end_pos)
        let chunk_indices = slice(indices, pos, chunk_end_pos)?;

        // Adjust the indices so they're relative to the chunk
        // Note. Indices might not have a dtype big enough to fit chunk_begin after cast,
        // if it does cast the scalar otherwise upcast the indices.
        let chunk_indices = if chunk_begin < PType::try_from(chunk_indices.dtype())?.max_value() {
            subtract_scalar(
                &chunk_indices,
                &Scalar::from(chunk_begin).cast(chunk_indices.dtype())?,
            )?
        } else {
            // Note. this try_cast (memory copy) is unnecessary, could instead upcast in the subtract fn.
            //  and avoid an extra
            let u64_chunk_indices = try_cast(&chunk_indices, PType::U64.into())
                .expect("safe to upcast since all indices are positive");

            subtract_scalar(&u64_chunk_indices, &chunk_begin.into())?
        };

        indices_by_chunk[chunk_idx] = Some(chunk_indices);

        pos = chunk_end_pos;
    }

    // Now we can take the chunks
    let chunks = indices_by_chunk
        .iter()
        .enumerate()
        .filter_map(|(chunk_idx, indices)| indices.as_ref().map(|i| (chunk_idx, i)))
        .map(|(chunk_idx, chunk_indices)| {
            take(
                &chunked.chunk(chunk_idx).expect("chunk not found"),
                chunk_indices,
            )
        })
        .try_collect()?;

    Ok(ChunkedArray::try_new(chunks, chunked.dtype().clone())?.into_array())
}

#[cfg(test)]
mod test {
    use crate::array::chunked::ChunkedArray;
    use crate::compute::take;
    use crate::{ArrayDType, AsArray, IntoArray, IntoArrayVariant};

    #[test]
    fn test_take() {
        let a = vec![1i32, 2, 3].into_array();
        let arr = ChunkedArray::try_new(vec![a.clone(), a.clone(), a.clone()], a.dtype().clone())
            .unwrap();
        assert_eq!(arr.nchunks(), 3);
        assert_eq!(arr.len(), 9);
        let indices = vec![0u64, 0, 6, 4].into_array();

        let result = &ChunkedArray::try_from(take(arr.as_array_ref(), &indices).unwrap())
            .unwrap()
            .into_array()
            .into_primitive()
            .unwrap();
        assert_eq!(result.maybe_null_slice::<i32>(), &[1, 1, 1, 2]);
    }
}
