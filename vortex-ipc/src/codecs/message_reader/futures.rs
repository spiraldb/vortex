#![cfg(feature = "futures")]
use std::io;

use bytes::{Bytes, BytesMut};
use flatbuffers::{root, root_unchecked};
use futures_util::{AsyncRead, AsyncReadExt, TryStreamExt};
use vortex::encoding::EncodingRef;
use vortex::Context;
use vortex_alp::ALPEncoding;
use vortex_buffer::Buffer;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_fastlanes::BitPackedEncoding;

use crate::codecs::ipc_reader::IPCReader;
use crate::codecs::message_reader::test::create_stream;
use crate::codecs::message_reader::MessageReader;
use crate::flatbuffers::ipc::Message;

pub struct AsyncReadMessageReader<R: AsyncRead + Unpin> {
    // TODO(ngates): swap this for our own mutable aligned buffer so we can support direct reads.
    read: R,
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

trait AsyncReadMoreExt: AsyncReadExt {
    async fn skip(&mut self, nbytes: u64) -> VortexResult<()>
    where
        Self: Unpin,
    {
        // TODO(ngates): can we grab dev/null? At the very least we should do this in small buffers.
        let mut bytes = BytesMut::zeroed(nbytes as usize);
        self.read_exact(bytes.as_mut()).await?;
        Ok(())
    }
}
impl<R: AsyncReadExt> AsyncReadMoreExt for R {}

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

    async fn buffers(&mut self) -> VortexResult<Vec<Buffer>> {
        let Some(chunk_msg) = unsafe { root_unchecked::<Message>(&self.message) }.header_as_chunk()
        else {
            // We could return an error here?
            return Ok(Vec::new());
        };

        // Read all the column's buffers
        let mut offset = 0;
        let mut buffers = Vec::with_capacity(chunk_msg.buffers().unwrap_or_default().len());
        for buffer in chunk_msg.buffers().unwrap_or_default().iter() {
            let _skip = buffer.offset() - offset;
            self.read.skip(buffer.offset() - offset).await?;

            // TODO(ngates): read into a single buffer, then Arc::clone and slice
            let mut bytes = BytesMut::zeroed(buffer.length() as usize);
            self.read.read_exact(bytes.as_mut()).await?;
            buffers.push(Buffer::from(bytes.freeze()));

            offset = buffer.offset() + buffer.length();
        }

        // Consume any remaining padding after the final buffer.
        let _buffer_size = chunk_msg.buffer_size();
        self.read.skip(chunk_msg.buffer_size() - offset).await?;

        Ok(buffers)
    }
}

#[tokio::test]
async fn test_something() -> VortexResult<()> {
    let buffer = create_stream();

    let ctx = Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
    let mut messages = AsyncReadMessageReader::try_new(buffer.as_slice()).await?;

    let mut reader = IPCReader::try_from_messages(&ctx, &mut messages).await?;
    while let Some(mut array) = reader.next().await? {
        println!("ARRAY");

        while let Some(chunk) = array.next().await? {
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

    let ctx = Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
    let mut messages = AsyncReadMessageReader::try_new(reader).await?;

    let mut reader = IPCReader::try_from_messages(&ctx, &mut messages).await?;
    while let Some(mut array) = reader.next().await? {
        println!("ARRAY");

        while let Some(chunk) = array.next().await? {
            println!("chunk {:?}", chunk);
        }
    }

    Ok(())
}
