use std::future::Future;

use vortex_buffer::Buffer;
use vortex_error::VortexResult;
mod futures;
mod monoio;
use crate::flatbuffers::ipc::Message;

#[allow(dead_code)]
pub trait MessageReader {
    fn peek(&self) -> Option<Message>;
    fn next(&mut self) -> impl Future<Output = VortexResult<Message>>;
    /// Fetch the buffers associated with this message.
    fn buffers(&mut self) -> impl Future<Output = VortexResult<Vec<Buffer>>>;
}

#[cfg(test)]
pub mod test {
    use std::io::Cursor;

    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::encoding::EncodingRef;
    use vortex::{ArrayDType, Context, IntoArray};
    use vortex_alp::ALPEncoding;
    use vortex_fastlanes::BitPackedEncoding;

    use crate::writer::StreamWriter;

    pub fn create_stream() -> Vec<u8> {
        let ctx = Context::default().with_encodings([
            &ALPEncoding as EncodingRef,
            &BitPackedEncoding as EncodingRef,
        ]);
        let array = PrimitiveArray::from(vec![0, 1, 2]).into_array();
        let chunked_array =
            ChunkedArray::try_new(vec![array.clone(), array.clone()], array.dtype().clone())
                .unwrap()
                .into_array();

        let mut buffer = vec![];
        let mut cursor = Cursor::new(&mut buffer);
        {
            let mut writer = StreamWriter::try_new(&mut cursor, &ctx).unwrap();
            writer.write_array(&array).unwrap();
            writer.write_array(&chunked_array).unwrap();
        }

        // Push some extra bytes to test that the reader is well-behaved and doesn't read past the
        // end of the stream.
        // let _ = cursor.write(b"hello").unwrap();

        buffer
    }
}
