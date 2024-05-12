use std::future::Future;

use itertools::Itertools;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;

use crate::ALIGNMENT;

pub mod futures;
pub mod monoio;
use crate::flatbuffers::ipc::Message;

pub trait MessageReader {
    fn peek(&self) -> Option<Message>;
    fn next(&mut self) -> impl Future<Output = VortexResult<Message>>;
    fn next_raw(&mut self) -> impl Future<Output = VortexResult<Buffer>>;
    fn read_into(
        &mut self,
        buffers: Vec<Vec<u8>>,
    ) -> impl Future<Output = VortexResult<Vec<Vec<u8>>>>;

    /// Fetch the buffers associated with this message.
    fn buffers(&mut self) -> impl Future<Output = VortexResult<Vec<Buffer>>> {
        async {
            let Some(chunk_msg) = self.peek().and_then(|m| m.header_as_chunk()) else {
                // We could return an error here?
                return Ok(Vec::new());
            };

            // Initialize the column's buffers for a vectored read.
            // To start with, we include the padding and then truncate the buffers after.
            // TODO(ngates): improve the flatbuffer format instead of storing offset/len per buffer.
            let buffers = chunk_msg
                .buffers()
                .unwrap_or_default()
                .iter()
                .map(|buffer| {
                    // FIXME(ngates): this assumes the next buffer offset == the aligned length of
                    //  the previous buffer. I will fix this by improving the flatbuffer format instead
                    //  of fiddling with the logic here.
                    let len_width_padding =
                        (buffer.length() as usize + (ALIGNMENT - 1)) & !(ALIGNMENT - 1);
                    // TODO(ngates): switch to use uninitialized
                    // TODO(ngates): allocate the entire thing in one go and then split
                    vec![0u8; len_width_padding]
                })
                .collect_vec();

            // Just sanity check the above
            assert_eq!(
                buffers.iter().map(|b| b.len()).sum::<usize>(),
                chunk_msg.buffer_size() as usize
            );

            // Issue a vectored read to fill all buffers
            let buffers: Vec<Vec<u8>> = self.read_into(buffers).await?;

            // Truncate each buffer to strip the padding.
            let buffers = buffers
                .into_iter()
                .zip(
                    self.peek()
                        .unwrap()
                        .header_as_chunk()
                        .unwrap()
                        .buffers()
                        .unwrap_or_default()
                        .iter(),
                )
                .map(|(mut vec, buf)| {
                    vec.truncate(buf.length() as usize);
                    Buffer::from(vec)
                })
                .collect_vec();

            Ok(buffers)
        }
    }
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
