pub use message_reader::*;
pub use message_writer::*;

pub mod chunked_reader;
pub mod file;
pub mod io;
mod message_reader;
mod message_writer;
mod messages;
pub mod stream_reader;
mod sync_message_reader;
pub mod writer;

pub const ALIGNMENT: usize = 64;

pub mod flatbuffers {
    pub use generated_footer::vortex::*;
    pub use generated_message::vortex::*;

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated_message {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/message.rs"));
    }

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated_footer {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/footer.rs"));
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
mod test {
    use std::sync::Arc;

    use futures_executor::block_on;
    use futures_util::{pin_mut, StreamExt, TryStreamExt};
    use futures_util::io::Cursor;
    use itertools::Itertools;

    use vortex::{ArrayDType, Context, IntoArray};
    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
    use vortex::encoding::ArrayEncoding;
    use vortex::stream::ArrayStreamExt;
    use vortex_error::VortexResult;

    use crate::io::FuturesAdapter;
    use crate::MessageReader;
    use crate::writer::ArrayWriter;

    fn write_ipc<A: IntoArray>(array: A) -> Vec<u8> {
        block_on(async {
            ArrayWriter::new(vec![])
                .write_array(array.into_array())
                .await
                .unwrap()
                .into_inner()
        })
    }

<<<<<<< HEAD:vortex-ipc/src/lib.rs
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
=======
    #[test]
    fn test_empty_index() -> VortexResult<()> {
>>>>>>> 7a239b3c (Add vortex file format):vortex-serde/src/lib.rs
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

        let result = block_on(async { result_iter.next().await.unwrap().unwrap() });
        println!("Taken {:?}", result);
        Ok(())
    }

    #[test]
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
