use futures_util::stream::unfold;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::slice::slice;
use vortex::compute::take::take;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{Array, ArrayDType};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexResult};

use crate::codecs::array_reader::ArrayReaderAdapter;
use crate::codecs::ArrayReader;

pub trait ArrayReaderTake: ArrayReader {
    fn take(self, indices: &Array) -> VortexResult<impl ArrayReader>
    where
        Self: Sized,
    {
        if !indices.is_empty() {
            if !indices.statistics().compute_is_sorted()? {
                vortex_bail!("Indices must be sorted to take from IPC stream")
            }

            if indices.statistics().compute_null_count()? > 0 {
                vortex_bail!("Indices must not contain nulls")
            }

            if !indices.dtype().is_int() {
                vortex_bail!("Indices must be integers")
            }

            if indices.dtype().is_signed_int()
                && indices.statistics().compute_as_cast::<i64>(Stat::Min)? < 0
            {
                vortex_bail!("Indices must be positive")
            }
        }

        let dtype = self.dtype().clone();
        let init = Take {
            reader: self,
            indices,
            row_offset: 0,
            indices_offset: 0,
        };

        Ok(ArrayReaderAdapter::new(
            dtype,
            unfold(init, |mut take| async move { take.next().await }),
        ))
    }
}
impl<R: ArrayReader> ArrayReaderTake for R {}

#[allow(dead_code)]
struct Take<'idx, R: ArrayReader> {
    reader: R,
    indices: &'idx Array,
    row_offset: usize,
    indices_offset: usize,
}

impl<'idx, R: ArrayReader> Take<'idx, R> {
    async fn next(&mut self) -> Option<(VortexResult<Array>, Take<'idx, R>)> {
        if self.indices.is_empty() {
            return Ok(None);
        }

        while let Some(batch) = self.reader.next()? {
            let curr_offset = self.row_offset;
            let left = search_sorted::<usize>(self.indices, curr_offset, SearchSortedSide::Left)?
                .to_index();
            let right = search_sorted::<usize>(
                self.indices,
                curr_offset + batch.len(),
                SearchSortedSide::Left,
            )?
            .to_index();

            self.row_offset += batch.len();

            if left == right {
                continue;
            }

            let indices_for_batch = slice(self.indices, left, right)?.flatten_primitive()?;
            let shifted_arr = match_each_integer_ptype!(indices_for_batch.ptype(), |$T| {
                subtract_scalar(&indices_for_batch.into_array(), &Scalar::from(curr_offset as $T))?
            });

            return take(&batch, &shifted_arr).map(Some);
        }
        Ok(None)
    }
}
