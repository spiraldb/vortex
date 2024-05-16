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

use crate::array_stream::ArrayStream;

#[pin_project]
pub struct TakeRows<'idx, R: ArrayStream> {
    #[pin]
    reader: R,
    indices: &'idx Array,
    row_offset: usize,
}

impl<'idx, R: ArrayStream> TakeRows<'idx, R> {
    #[allow(dead_code)]
    pub fn try_new(reader: R, indices: &'idx Array) -> VortexResult<Self> {
        if !indices.is_empty() {
            if !indices.statistics().compute_is_sorted().unwrap_or(false) {
                vortex_bail!("Indices must be sorted to take from IPC stream")
            }

            if indices
                .statistics()
                .compute_null_count()
                .map(|nc| nc > 0)
                .unwrap_or(true)
            {
                vortex_bail!("Indices must not contain nulls")
            }

            if !indices.dtype().is_int() {
                vortex_bail!("Indices must be integers")
            }

            if indices.dtype().is_signed_int()
                && indices
                    .statistics()
                    .compute_as_cast::<i64>(Stat::Min)
                    .map(|min| min < 0)
                    .unwrap_or(true)
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

impl<'idx, R: ArrayStream> Stream for TakeRows<'idx, R> {
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
    use futures_util::{pin_mut, StreamExt, TryStreamExt};
    use itertools::Itertools;
    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
    use vortex::encoding::ArrayEncoding;
    use vortex::{ArrayDType, Context, IntoArray, ViewContext};
    use vortex_error::VortexResult;

    use crate::array_stream::ArrayStreamExt;
    use crate::io::FuturesVortexRead;
    use crate::stream_writer::ArrayWriter;
    use crate::MessageReader;

    async fn write_array<A: IntoArray>(array: A) -> Vec<u8> {
        let mut writer = ArrayWriter::new(vec![], ViewContext::default());
        writer.write_context().await.unwrap();
        writer.write_array(array.into_array()).await.unwrap();
        writer.into_write()
    }

    #[tokio::test]
    async fn test_empty_index() -> VortexResult<()> {
        let data = PrimitiveArray::from((0i32..3_000_000).collect_vec());
        let buffer = write_array(data).await;

        let indices = PrimitiveArray::from(vec![1, 2, 10]).into_array();

        ArrayReader

        let ctx = Context::default();
        let mut messages = MessageReader::try_new(FuturesVortexRead(Cursor::new(buffer)))
            .await
            .unwrap();
        let reader = messages.array_stream_from_messages(&ctx).await?;

        let result_iter = reader.take_rows(&indices).unwrap();
        pin_mut!(result_iter);

        let result = result_iter.next().await.unwrap().unwrap();
        println!("Taken {:?}", result);
        Ok(())
    }

    #[tokio::test]
    async fn test_write_read_chunked() -> VortexResult<()> {
        let indices = PrimitiveArray::from(vec![
            10u32, 11, 12, 13, 100_000, 2_999_999, 2_999_999, 3_000_000,
        ])
        .into_array();

        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let data2 =
            PrimitiveArray::from((3_000_000i32..6_000_000).rev().collect_vec()).into_array();
        let chunked = ChunkedArray::try_new(vec![data.clone(), data2], data.dtype().clone())?;
        let buffer = write_ipc(chunked);

        let mut messages = MessageReader::try_new(FuturesVortexRead(Cursor::new(buffer))).await?;

        let ctx = Context::default();
        let take_iter = messages
            .array_stream_from_messages(&ctx)
            .await?
            .take_rows(&indices)?;
        pin_mut!(take_iter);

        let next = take_iter.try_next().await?.expect("Expected a chunk");
        assert_eq!(next.encoding().id(), PrimitiveEncoding.id());

        assert_eq!(
            next.into_primitive().typed_data::<i32>(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
        assert_eq!(
            take_iter
                .try_next()
                .await?
                .expect("Expected a chunk")
                .into_primitive()
                .typed_data::<i32>(),
            vec![5999999]
        );

        Ok(())
    }
}
