#![cfg(feature = "futures")]

use std::io;

use bytes::BytesMut;
use flatbuffers::{root, root_unchecked};
use futures_util::{AsyncRead, AsyncReadExt};
use vortex_buffer::Buffer;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::codecs::message_reader::MessageReader;
use crate::flatbuffers::ipc::Message;

pub struct AsyncReadMessageReader<R: AsyncRead + Unpin> {
    read: R,
    // TODO(ngates): swap this for our own mutable aligned buffer so we can support direct reads.
    message: BytesMut,
    prev_message: BytesMut,
    finished: bool,
}

impl<R: AsyncRead + Unpin> AsyncReadMessageReader<R> {
    pub async fn try_new(read: R) -> VortexResult<Self> {
        let mut reader = Self {
            read,
            message: BytesMut::new(),
            prev_message: BytesMut::new(),
            finished: false,
        };
        reader.load_next_message().await?;
        Ok(reader)
    }

    async fn load_next_message(&mut self) -> VortexResult<bool> {
        let mut len_buf = [0u8; 4];
        match self.read.read_exact(&mut len_buf).await {
            Ok(()) => {}
            Err(e) => {
                return match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e.into()),
                };
            }
        }

        let len = u32::from_le_bytes(len_buf);
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        } else if len == 0 {
            vortex_bail!(InvalidSerde: "Invalid IPC stream")
        }

        let mut message = BytesMut::zeroed(len as usize);
        self.read.read_exact(message.as_mut()).await?;

        self.message = message;

        // Validate that the message is valid a flatbuffer.
        root::<Message>(&self.message).map_err(
            |e| vortex_err!(InvalidSerde: "Failed to parse flatbuffer message: {:?}", e),
        )?;

        Ok(true)
    }
}

impl<R: AsyncRead + Unpin> MessageReader for AsyncReadMessageReader<R> {
    fn peek(&self) -> Option<Message> {
        if self.finished {
            return None;
        }
        // The message has been validated by the next() call.
        Some(unsafe { root_unchecked::<Message>(&self.message) })
    }

    async fn next(&mut self) -> VortexResult<Message> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        self.prev_message = self.message.split();
        if !self.load_next_message().await? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<Message>(&self.prev_message) })
    }

    async fn next_raw(&mut self) -> VortexResult<Buffer> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        self.prev_message = self.message.split();
        if !self.load_next_message().await? {
            self.finished = true;
        }
        Ok(Buffer::from(self.prev_message.clone().freeze()))
    }

    async fn read_into(&mut self, mut buffers: Vec<Vec<u8>>) -> VortexResult<Vec<Vec<u8>>> {
        // TODO(ngates): there is no read_vectored_exact for AsyncRead, so for now we'll
        //  just read one-by-one
        for buffer in buffers.iter_mut() {
            self.read.read_exact(buffer).await?;
        }
        Ok(buffers)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use vortex::encoding::EncodingRef;
    use vortex::Context;
    use vortex_alp::ALPEncoding;
    use vortex_fastlanes::BitPackedEncoding;

    use super::*;
    use crate::codecs::array_reader::ArrayReader;
    use crate::codecs::ipc_reader::IPCReader;
    use crate::codecs::message_reader::test::create_stream;

    #[tokio::test]
    async fn test_something() -> VortexResult<()> {
        let buffer = create_stream();

        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
        let mut messages = AsyncReadMessageReader::try_new(buffer.as_slice()).await?;

        let mut reader = IPCReader::try_from_messages(&ctx, &mut messages).await?;
        while let Some(array) = reader.next().await? {
            futures_util::pin_mut!(array);
            println!("ARRAY: {}", array.dtype());
            while let Some(chunk) = array.try_next().await? {
                println!("chunk {:?}", chunk);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_stream() -> VortexResult<()> {
        let buffer = create_stream();

        let stream = futures_util::stream::iter(
            buffer
                .chunks(64)
                .map(|chunk| Ok(Bytes::from(chunk.to_vec()))),
        );
        let reader = stream.into_async_read();

        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
        let mut messages = AsyncReadMessageReader::try_new(reader).await?;

        let mut reader = IPCReader::try_from_messages(&ctx, &mut messages).await?;
        while let Some(array) = reader.next().await? {
            futures_util::pin_mut!(array);
            println!("ARRAY {}", array.dtype());
            while let Some(chunk) = array.try_next().await? {
                println!("chunk {:?}", chunk);
            }
        }

        Ok(())
    }
}
