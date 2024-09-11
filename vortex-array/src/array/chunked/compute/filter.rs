use vortex_error::{VortexExpect, VortexResult};

use crate::array::{ChunkedArray, PrimitiveArray};
use crate::compute::{slice, take, FilterFn};
use crate::{Array, ArrayDType, IntoArray};

// This is modeled after the constant with the equivalent name in arrow-rs.
const FILTER_SLICES_SELECTIVITY_THRESHOLD: f64 = 0.8;

impl FilterFn for ChunkedArray {
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        predicate.with_dyn(move |a| {
            // SAFETY: the DType should be checked in the top-level `filter` function.
            let bool_array = a.as_bool_array_unchecked();
            let selected = bool_array.true_count();

            if selected == self.len() {
                // Fast path 1: no filtering
                Ok(self.clone().into_array())
            } else if selected == 0 {
                // Fast path 2: empty array after filter.
                Ok(ChunkedArray::try_new(vec![], self.dtype().clone())?.into_array())
            } else {
                // General path: perform filtering.
                //
                // Based on filter selectivity, we take the values between a range of slices, or
                // we take individual indices.
                let selectivity = selected as f64 / self.len() as f64;
                let chunks = if selectivity > FILTER_SLICES_SELECTIVITY_THRESHOLD {
                    filter_slices(self, bool_array.maybe_null_slices_iter())?
                } else {
                    filter_indices(self, bool_array.maybe_null_indices_iter())?
                };

                Ok(ChunkedArray::try_new(chunks, self.dtype().clone())?.into_array())
            }
        })
    }
}

/// Filter the chunks using slice ranges.
fn filter_slices<'a>(
    array: &'a ChunkedArray,
    set_slices: Box<dyn Iterator<Item = (usize, usize)> + 'a>,
) -> VortexResult<Vec<Array>> {
    let mut result = Vec::with_capacity(array.nchunks());

    for (slice_start, slice_end) in set_slices {
        // Find the chunk between begin and end.
        let (start_chunk, start_idx) = array.find_chunk_idx(slice_start);
        let (end_chunk, end_idx) = array.find_chunk_idx(slice_end);

        // Accumulate some number of chunks into the chunks buffer.
        if start_chunk == end_chunk {
            let chunk = array
                .chunk(start_chunk)
                .vortex_expect("chunk_idx must be in range");
            result.push(slice(&chunk, start_idx, end_idx)?);
        }

        for chunk_id in start_chunk..end_chunk {
            let chunk = array
                .chunk(chunk_id)
                .vortex_expect("find_chunk_idx must return valid chunk ID");
            let start = if chunk_id == start_chunk {
                start_idx
            } else {
                0
            };

            let end = if chunk_id == end_chunk {
                end_idx
            } else {
                chunk.len()
            };

            result.push(slice(&chunk, start, end)?);
        }
    }

    Ok(result)
}

/// Filter the chunks using indices.
fn filter_indices<'a>(
    array: &'a ChunkedArray,
    set_indices: Box<dyn Iterator<Item = usize> + 'a>,
) -> VortexResult<Vec<Array>> {
    let mut result = Vec::new();
    let mut current_chunk_id = 0;
    let mut chunk_indices = Vec::new();
    for set_index in set_indices {
        let (chunk_id, index) = array.find_chunk_idx(set_index);
        if chunk_id != current_chunk_id {
            // Push the chunk we've accumulated.
            if !chunk_indices.is_empty() {
                let chunk = array
                    .chunk(current_chunk_id)
                    .vortex_expect("find_chunk_idx must return valid chunk ID");
                let filtered_chunk = take(
                    &chunk,
                    &PrimitiveArray::from(chunk_indices.clone()).into_array(),
                )?;
                result.push(filtered_chunk);
            }

            // Advance the chunk forward, reset the chunk buffer.
            current_chunk_id = chunk_id;
            chunk_indices.clear();
        }

        chunk_indices.push(index as u64);
    }

    if !chunk_indices.is_empty() {
        let chunk = array
            .chunk(current_chunk_id)
            .vortex_expect("find_chunk_idx must return valid chunk ID");
        let filtered_chunk = take(
            &chunk,
            &PrimitiveArray::from(chunk_indices.clone()).into_array(),
        )?;
        result.push(filtered_chunk);
    }

    Ok(result)
}
