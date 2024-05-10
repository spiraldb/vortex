use std::io;

use bytes::BytesMut;
use flatbuffers::{root, root_unchecked};
use futures_util::{AsyncRead, AsyncReadExt};
use vortex_error::VortexResult;

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
                println!("ERROR READING LENGTH");
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
        }

        assert_eq!(self.message.len(), 0);
        self.message.resize(len as usize, 0);
        println!("MESSAGE {}", self.message.len());
        self.read.read_exact(&mut self.message).await?;

        // Validate that the message is valid a flatbuffer.
        let _ = root::<Message>(&self.message)?;
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
}

#[tokio::test]
async fn test_something() -> VortexResult<()> {
    let buffer = create_stream();

    // TODO(ngates): stream
    // let _stream = buffer
    //     .into_iter()
    //     .chunks(64)
    //     .into_iter()
    //     .map(|chunk| chunk.collect::<Vec<u8>>());

    let mut reader = AsyncReadMessageReader::try_new(buffer.as_slice()).await?;
    while reader.peek().is_some() {
        let msg = reader.next().await?;
        println!("MSG {:?}", msg);
    }

    Ok(())
}
