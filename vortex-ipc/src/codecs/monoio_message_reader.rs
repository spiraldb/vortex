#![cfg(feature = "monoio")]
#![allow(dead_code)]

use bytes::BytesMut;
use flatbuffers::{root, root_unchecked};
use monoio::buf::IoBufMut;
use monoio::io::{AsyncReadRent, AsyncReadRentExt};
use vortex_error::VortexResult;

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
}

trait AsyncReadRentMoreExt: AsyncReadRentExt {
    /// Same as read_exact except unwraps the BufResult into a regular IO result.
    async fn read_exact_into<B: IoBufMut>(&mut self, buf: B) -> std::io::Result<B> {
        match self.read_exact(buf).await {
            (Ok(_), buf) => Ok(buf),
            (Err(e), _) => Err(e),
        }
    }
}

impl<R: AsyncReadRentExt> AsyncReadRentMoreExt for R {}

#[monoio::test]
async fn test_something() -> VortexResult<()> {
    let buffer = create_stream();

    // TODO(ngates): stream
    // let _stream = buffer
    //     .into_iter()
    //     .chunks(64)
    //     .into_iter()
    //     .map(|chunk| chunk.collect::<Vec<u8>>());

    let mut reader = MonoIoMessageReader::try_new(buffer.as_slice()).await?;
    while reader.peek().is_some() {
        let msg = reader.next().await?;
        println!("MSG {:?}", msg);
    }

    Ok(())
}
