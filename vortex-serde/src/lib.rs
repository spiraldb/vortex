pub use message_reader::*;
pub use message_writer::*;

pub mod chunked_reader;
pub mod io;
pub mod layouts;
mod message_reader;
mod message_writer;
mod messages;
pub mod stream_reader;
pub mod writer;

pub const ALIGNMENT: usize = 64;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use futures_executor::block_on;
    use futures_util::io::Cursor;
    use futures_util::{pin_mut, StreamExt, TryStreamExt};
    use itertools::Itertools;
    use vortex::array::{ChunkedArray, PrimitiveArray, PrimitiveEncoding};
    use vortex::encoding::ArrayEncoding;
    use vortex::stream::ArrayStreamExt;
    use vortex::{ArrayDType, Context, IntoArray};
    use vortex_error::VortexResult;

    use crate::io::FuturesAdapter;
    use crate::writer::ArrayWriter;
    use crate::MessageReader;

    fn write_ipc<A: IntoArray>(array: A) -> Vec<u8> {
        block_on(async {
            ArrayWriter::new(vec![])
                .write_array(array.into_array())
                .await
                .unwrap()
                .into_inner()
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_empty_index() -> VortexResult<()> {
        let data = PrimitiveArray::from((0i32..3_000_000).collect_vec());
        let buffer = write_ipc(data);

        let indices = PrimitiveArray::from(vec![1, 2, 10]).into_array();

        let ctx = Arc::new(Context::default());
        let mut messages = block_on(async {
            MessageReader::try_new(FuturesAdapter(Cursor::new(buffer)))
                .await
                .unwrap()
        });
        let reader = block_on(async { messages.array_stream_from_messages(ctx).await })?;

        let result_iter = reader.take_rows(indices).unwrap();
        pin_mut!(result_iter);

        let _result = block_on(async { result_iter.next().await.unwrap().unwrap() });
        Ok(())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_write_read_chunked() -> VortexResult<()> {
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

        let mut messages =
            block_on(async { MessageReader::try_new(FuturesAdapter(Cursor::new(buffer))).await })?;

        let ctx = Arc::new(Context::default());
        let take_iter = block_on(async { messages.array_stream_from_messages(ctx).await })?
            .take_rows(indices)?;
        pin_mut!(take_iter);

        let next = block_on(async { take_iter.try_next().await })?.expect("Expected a chunk");
        assert_eq!(next.encoding().id(), PrimitiveEncoding.id());

        assert_eq!(
            next.as_primitive().maybe_null_slice::<i32>(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
        assert_eq!(
            block_on(async { take_iter.try_next().await })?
                .expect("Expected a chunk")
                .as_primitive()
                .maybe_null_slice::<i32>(),
            vec![5999999]
        );

        Ok(())
    }
}
