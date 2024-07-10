pub use message_reader::*;
pub use message_writer::*;

pub mod chunked_reader;
pub mod io;
mod message_reader;
mod message_writer;
mod messages;
pub mod stream_reader;
pub mod writer;

pub const ALIGNMENT: usize = 64;

pub mod flatbuffers {
    pub use generated::vortex::*;

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/message.rs"));
    }

    mod deps {
        pub mod array {
            pub use vortex::flatbuffers as array;
        }

        pub mod dtype {
            pub use vortex_dtype::flatbuffers as dtype;
        }

        pub mod scalar {
            #[allow(unused_imports)]
            pub use vortex_scalar::flatbuffers as scalar;
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::sync::Arc;

    use futures_util::io::Cursor;
    use futures_util::{pin_mut, StreamExt, TryStreamExt};
    use itertools::Itertools;
    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
    use vortex::encoding::ArrayEncoding;
    use vortex::stream::ArrayStreamExt;
    use vortex::{ArrayDType, Context, IntoArray};
    use vortex_error::VortexResult;

    use crate::io::FuturesAdapter;
    use crate::writer::ArrayWriter;
    use crate::MessageReader;

    pub async fn create_stream() -> Vec<u8> {
        let array = PrimitiveArray::from(vec![0, 1, 2]).into_array();
        let chunked_array =
            ChunkedArray::try_new(vec![array.clone(), array.clone()], array.dtype().clone())
                .unwrap()
                .into_array();

        ArrayWriter::new(vec![])
            .write_array(array)
            .await
            .unwrap()
            .write_array(chunked_array)
            .await
            .unwrap()
            .into_inner()
    }

    async fn write_ipc<A: IntoArray>(array: A) -> Vec<u8> {
        ArrayWriter::new(vec![])
            .write_array(array.into_array())
            .await
            .unwrap()
            .into_inner()
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_empty_index() -> VortexResult<()> {
        let data = PrimitiveArray::from((0i32..3_000_000).collect_vec());
        let buffer = write_ipc(data).await;

        let indices = PrimitiveArray::from(vec![1, 2, 10]).into_array();

        let ctx = Arc::new(Context::default());
        let mut messages = MessageReader::try_new(FuturesAdapter(Cursor::new(buffer)))
            .await
            .unwrap();
        let reader = messages.array_stream_from_messages(ctx).await?;

        let result_iter = reader.take_rows(indices).unwrap();
        pin_mut!(result_iter);

        let result = result_iter.next().await.unwrap().unwrap();
        println!("Taken {:?}", result);
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
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
        let buffer = write_ipc(chunked).await;

        let mut messages = MessageReader::try_new(FuturesAdapter(Cursor::new(buffer))).await?;

        let ctx = Arc::new(Context::default());
        let take_iter = messages
            .array_stream_from_messages(ctx)
            .await?
            .take_rows(indices)?;
        pin_mut!(take_iter);

        let next = take_iter.try_next().await?.expect("Expected a chunk");
        assert_eq!(next.encoding().id(), PrimitiveEncoding.id());

        assert_eq!(
            next.as_primitive().maybe_null_slice::<i32>(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
        assert_eq!(
            take_iter
                .try_next()
                .await?
                .expect("Expected a chunk")
                .as_primitive()
                .maybe_null_slice::<i32>(),
            vec![5999999]
        );

        Ok(())
    }
}
