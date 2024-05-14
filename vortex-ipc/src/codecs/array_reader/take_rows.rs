use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{ready, Stream};
use pin_project::pin_project;
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

use crate::codecs::ArrayReader;

#[pin_project]
pub struct TakeRows<'idx, R: ArrayReader> {
    #[pin]
    reader: R,
    indices: &'idx Array,
    row_offset: usize,
}

impl<'idx, R: ArrayReader> TakeRows<'idx, R> {
    #[allow(dead_code)]
    pub fn try_new(reader: R, indices: &'idx Array) -> VortexResult<Self> {
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

        Ok(Self {
            reader,
            indices,
            row_offset: 0,
        })
    }
}

impl<'idx, R: ArrayReader> Stream for TakeRows<'idx, R> {
    type Item = VortexResult<Array>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if this.indices.is_empty() {
            return Poll::Ready(None);
        }

        while let Some(batch) = ready!(this.reader.as_mut().poll_next(cx)?) {
            let curr_offset = *this.row_offset;
            let left = search_sorted::<usize>(this.indices, curr_offset, SearchSortedSide::Left)?
                .to_index();
            let right = search_sorted::<usize>(
                this.indices,
                curr_offset + batch.len(),
                SearchSortedSide::Left,
            )?
            .to_index();

            *this.row_offset += batch.len();

            if left == right {
                continue;
            }

            // TODO(ngates): this is probably too heavy to run on the event loop. We should spawn
            //  onto a worker pool.
            let indices_for_batch = slice(this.indices, left, right)?.flatten_primitive()?;
            let shifted_arr = match_each_integer_ptype!(indices_for_batch.ptype(), |$T| {
                subtract_scalar(&indices_for_batch.into_array(), &Scalar::from(curr_offset as $T))?
            });
            return Poll::Ready(take(&batch, &shifted_arr).map(Some).transpose());
        }

        Poll::Ready(None)
    }
}

#[cfg(test)]
mod test {
    use futures_util::io::Cursor;
    use futures_util::{pin_mut, StreamExt};
    use itertools::Itertools;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::{Context, IntoArray};

    use crate::codecs::array_reader::ext::ArrayReaderExt;
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
        let data = PrimitiveArray::from((0i32..3_000_000).collect_vec());
        let buffer = write_ipc(data);

        let indices = PrimitiveArray::from(vec![1, 2, 10]).into_array();

        let mut messages = AsyncReadMessageReader::try_new(Cursor::new(buffer))
            .await
            .unwrap();
        let mut reader = IPCReader::try_from_messages(&Context::default(), &mut messages)
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();

        let result_iter = array_reader.take_rows(&indices).unwrap();
        pin_mut!(result_iter);

        let result = result_iter.next().await.unwrap().unwrap();
        println!("Taken {:?}", result);
    }
}
