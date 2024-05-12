use std::pin::Pin;

use futures_util::stream::try_unfold;
use futures_util::TryStreamExt;
use vortex::compute::scalar_subtract::subtract_scalar;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::slice::slice;
use vortex::compute::take::take;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::IntoArray;
use vortex::{Array, ArrayDType};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::codecs::array_reader::ArrayReaderAdapter;
use crate::codecs::ArrayReader;

pub trait ArrayReaderTake: ArrayReader {
    fn take_indices(self, indices: &Array) -> VortexResult<impl ArrayReader>
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
        };

        Ok(ArrayReaderAdapter::new(
            dtype,
            try_unfold(init, |mut take| async move {
                let batch = take.next().await?;
                Ok(batch.map(|b| (b, take)))
            }),
        ))
    }
}
impl<R: ArrayReader + Unpin> ArrayReaderTake for R {}

struct Take<'idx, R: ArrayReader> {
    reader: R,
    indices: &'idx Array,
    row_offset: usize,
}

impl<'idx, R: ArrayReader> Take<'idx, R> {
    async fn next(self: Pin<&mut Self>) -> VortexResult<Option<Array>> {
        if self.indices.is_empty() {
            return Ok(None);
        }

        let this = self.project();

        while let Some(batch) = self.reader.try_next().await? {
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

#[cfg(test)]
mod test {

    use futures_util::io::Cursor;
    use itertools::Itertools;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::{Context, IntoArray};

    use crate::codecs::array_reader_take::ArrayReaderTake;
    use crate::codecs::futures::AsyncReadMessageReader;
    use crate::codecs::IPCReader;
    use crate::writer::StreamWriter;

    fn write_ipc<A: IntoArray>(array: A) -> Vec<u8> {
        let mut buffer = vec![];
        let mut cursor = std::io::Cursor::new(&mut buffer);
        {
            let mut writer = StreamWriter::try_new(&mut cursor, &Context::default()).unwrap();
            writer.write_array(&array.into_array()).unwrap();
        }
        buffer
    }

    #[tokio::test]
    async fn test_empty_index() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec());
        let mut buffer = write_ipc(data);

        let indices = PrimitiveArray::from(Vec::<i32>::new()).into_array();

        let mut messages = AsyncReadMessageReader::try_new(Cursor::new(buffer))
            .await
            .unwrap();
        let mut reader = IPCReader::try_from_messages(&Context::default(), &mut messages)
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();
        futures_util::pin_mut!(array_reader);

        let mut result_iter = array_reader.take_indices(&indices).unwrap();
        let result = result_iter.next().await.unwrap();
        assert!(result.is_none())
    }
}
