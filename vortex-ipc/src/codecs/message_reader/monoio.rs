#![cfg(feature = "monoio")]
#![allow(dead_code)]

use bytes::BytesMut;
use flatbuffers::{root, root_unchecked};
use futures_util::TryStreamExt;
use monoio::buf::IoBufMut;
use monoio::io::{AsyncReadRent, AsyncReadRentExt};
use vortex::encoding::EncodingRef;
use vortex::Context;
use vortex_alp::ALPEncoding;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_fastlanes::BitPackedEncoding;

use crate::codecs::array_reader::ArrayReader;
use crate::codecs::ipc_reader::IPCReader;
use crate::codecs::message_reader::test::create_stream;
use crate::codecs::message_reader::MessageReader;
use crate::flatbuffers::ipc::Message;

struct MonoIoMessageReader<R: AsyncReadRent + Unpin> {
    // TODO(ngates): swap this for our own mutable aligned buffer so we can support direct reads.
    read: R,
    message: BytesMut,
    prev_message: BytesMut,
    finished: bool,
}

impl<R: AsyncReadRent + Unpin> MonoIoMessageReader<R> {
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
        // FIXME(ngates): how do we read into a stack allocated thing?
        let len_buf = self.read.read_exact_into(Vec::with_capacity(4)).await?;

        let len = u32::from_le_bytes(len_buf.as_slice().try_into()?);
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        }

        // TODO(ngates): we may be able to use self.message.split() and then swap back after.

        let message = self
            .read
            .read_exact_into(BytesMut::with_capacity(len as usize))
            .await?;

        // Validate that the message is valid a flatbuffer.
        let _ = root::<Message>(message.as_ref())?;

        self.message = message;

        Ok(true)
    }
}

impl<R: AsyncReadRent + Unpin> MessageReader for MonoIoMessageReader<R> {
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
            let bytes = BytesMut::zeroed(buffer.length() as usize);
            let bytes = self.read.read_exact_into(bytes).await?;
            buffers.push(Buffer::from(bytes.freeze()));

            offset = buffer.offset() + buffer.length();
        }

        // Consume any remaining padding after the final buffer.
        self.read.skip(chunk_msg.buffer_size() - offset).await?;

        Ok(buffers)
    }
}

trait AsyncReadRentMoreExt: AsyncReadRentExt {
    /// Same as read_exact except unwraps the BufResult into a regular IO result.
    async fn read_exact_into<B: IoBufMut>(&mut self, buf: B) -> std::io::Result<B> {
        match self.read_exact(buf).await {
            (Ok(_), buf) => Ok(buf),
            (Err(e), _) => Err(e),
        }
    }

    async fn skip(&mut self, nbytes: u64) -> VortexResult<()> {
        let _ = self
            .read_exact_into(BytesMut::zeroed(nbytes as usize))
            .await?;
        Ok(())
    }
}

impl<R: AsyncReadRentExt> AsyncReadRentMoreExt for R {}

#[monoio::test]
async fn test_something() -> VortexResult<()> {
    let buffer = create_stream();

    let ctx = Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
    let mut messages = MonoIoMessageReader::try_new(buffer.as_slice()).await?;

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

#[monoio::test]
async fn test_array_stream() -> VortexResult<()> {
    let buffer = create_stream();

    let ctx = Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
    let mut messages = MonoIoMessageReader::try_new(buffer.as_slice()).await?;

    let mut reader = IPCReader::try_from_messages(&ctx, &mut messages).await?;
    while let Some(array) = reader.next().await? {
        futures_util::pin_mut!(array);
        while let Some(array) = array.try_next().await? {
            println!("chunk {:?}", array);
        }
    }

    Ok(())
}
