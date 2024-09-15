use arrow_buffer::BooleanBufferBuilder;
use vortex_error::{VortexExpect, VortexResult};

use crate::array::{find_chunk_idx, BoolArray, ChunkedArray, PrimitiveArray};
use crate::compute::unary::scalar_at;
use crate::compute::{filter, take, FilterFn};
use crate::{Array, ArrayDType, IntoArray, IntoCanonical};

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

/// The filter to apply to each chunk.
///
/// When we rewrite a set of slices in a filter predicate into chunk addresses, we want to account
/// for the fact that some chunks will be wholly skipped.
#[derive(Clone)]
enum ChunkFilter {
    All,
    None,
    Slices(Vec<(usize, usize)>),
}

/// Given a sequence of slices that indicate ranges of set values, returns a boolean array
/// representing the same thing.
fn slices_to_predicate(slices: &[(usize, usize)], len: usize) -> Array {
    let mut buffer = BooleanBufferBuilder::new(len);

    let mut pos = 0;
    for (slice_start, slice_end) in slices.iter().copied() {
        // write however many trailing `false` between the end of the previous slice and the
        // start of this one.
        let n_leading_false = slice_start - pos;
        buffer.append_n(n_leading_false, false);
        buffer.append_n(slice_end - slice_start, true);
        pos = slice_end;
    }

    // Pad the end of the buffer with false, if necessary.
    let n_trailing_false = len - pos;
    buffer.append_n(n_trailing_false, false);

    BoolArray::from(buffer.finish()).into_array()
}

/// Filter the chunks using slice ranges.
fn filter_slices<'a>(
    array: &'a ChunkedArray,
    set_slices: Box<dyn Iterator<Item = (usize, usize)> + 'a>,
) -> VortexResult<Vec<Array>> {
    let mut result = Vec::with_capacity(array.nchunks());

    // Pre-materialize the chunk ends to avoid decompressing in hot loop.
    let chunk_ends = array.chunk_offsets().into_canonical()?.into_primitive()?;

    let mut chunk_filters = vec![ChunkFilter::None; array.nchunks()];

    for (slice_start, slice_end) in set_slices {
        let (start_chunk, start_idx) = find_chunk_idx(chunk_ends.array(), slice_start);
        // NOTE: we adjust slice end back by one, in case it ends on a chunk boundary, we do not
        // want to index into the unused chunk.
        let (end_chunk, end_idx) = find_chunk_idx(chunk_ends.array(), slice_end - 1);
        // Adjust back to an exclusive range
        let end_idx = end_idx + 1;

        if start_chunk == end_chunk {
            // start == end means that the slice lies within a single chunk.
            match &mut chunk_filters[start_chunk] {
                f @ (ChunkFilter::All | ChunkFilter::None) => {
                    *f = ChunkFilter::Slices(vec![(start_idx, end_idx)]);
                }
                ChunkFilter::Slices(slices) => {
                    slices.push((start_idx, end_idx));
                }
            }
        } else {
            // start != end means that the range is split over at least two chunks:
            // start chunk: append a slice from (start_idx, start_chunk_end).
            // end chunk: append a slice from (0, end_idx).
            // chunks between start and end: append ChunkFilter::All.
            let start_chunk_end: u64 =
                scalar_at(chunk_ends.array(), start_chunk + 1)?.try_into()?;
            let start_slice = (start_idx, start_chunk_end as _);
            match &mut chunk_filters[start_chunk] {
                f @ (ChunkFilter::All | ChunkFilter::None) => {
                    *f = ChunkFilter::Slices(vec![start_slice])
                }
                ChunkFilter::Slices(slices) => slices.push(start_slice),
            }

            let end_slice = (0, end_idx);
            match &mut chunk_filters[end_chunk] {
                f @ (ChunkFilter::All | ChunkFilter::None) => {
                    *f = ChunkFilter::Slices(vec![end_slice]);
                }
                ChunkFilter::Slices(slices) => slices.push(end_slice),
            }

            #[allow(clippy::needless_range_loop)]
            for chunk in (start_chunk + 1)..end_chunk {
                chunk_filters[chunk] = ChunkFilter::All;
            }
        }
    }

    // Now, apply the chunk filter to every slice.
    for (chunk, chunk_filter) in array.chunks().zip(chunk_filters.iter()) {
        match chunk_filter {
            // All => preserve the entire chunk unfiltered.
            ChunkFilter::All => result.push(chunk),
            // None => whole chunk is filtered out, skip
            ChunkFilter::None => {}
            // Slices => turn the slices into a boolean buffer.
            ChunkFilter::Slices(slices) => {
                result.push(filter(&chunk, &slices_to_predicate(slices, chunk.len()))?);
            }
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

    // Avoid find_chunk_idx and use our own to avoid the overhead.
    // The array should only be some thousands of values in the general case.
    let chunk_ends = array.chunk_offsets().into_canonical()?.into_primitive()?;

    for set_index in set_indices {
        let (chunk_id, index) = find_chunk_idx(chunk_ends.array(), set_index);
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

            // Advance the chunk forward, reset the chunk indices buffer.
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

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use crate::array::chunked::compute::filter::slices_to_predicate;
    use crate::IntoArrayVariant;

    #[test]
    fn test_slices_to_predicate() {
        let slices = [(2, 4), (6, 8), (9, 10)];
        let predicate = slices_to_predicate(&slices, 11);

        let bools = predicate
            .into_bool()
            .unwrap()
            .boolean_buffer()
            .iter()
            .collect_vec();

        assert_eq!(
            bools,
            vec![false, false, true, true, false, false, true, true, false, true, false],
        )
    }
}
