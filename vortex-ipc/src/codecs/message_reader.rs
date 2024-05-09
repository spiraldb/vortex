use std::io;

use bytes::BytesMut;
use flatbuffers::{root, root_unchecked};
use futures_util::{AsyncRead, AsyncReadExt};
use vortex_error::VortexResult;

use crate::flatbuffers::ipc::Message;

struct AsyncReadMessageReader<R: AsyncRead + Unpin> {
    // TODO(ngates): swap this for our own mutable aligned buffer so we can support direct reads.
    message: BytesMut,
    prev_message: BytesMut,
    finished: bool,
}

impl<R: AsyncRead + Unpin> AsyncReadMessageReader<R> {
    pub async fn try_new(read: &mut R) -> VortexResult<Self> {
        let mut reader = Self {
            message: BytesMut::new(),
            prev_message: BytesMut::new(),
            finished: false,
        };
        reader.load_next_message(read).await?;
        Ok(reader)
    }

    pub fn peek(&self) -> Option<Message> {
        if self.finished {
            return None;
        }
        // The message has been validated by the next() call.
        Some(unsafe { root_unchecked::<Message>(&self.message) })
    }

    pub async fn next(&mut self, read: &mut R) -> VortexResult<Message> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        self.prev_message = self.message.split();
        if !self.load_next_message(read).await? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<Message>(&self.prev_message) })
    }

    async fn load_next_message(&mut self, read: &mut R) -> VortexResult<bool> {
        let mut len_buf = [0u8; 4];
        match read.read_exact(&mut len_buf).await {
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
        }

        assert_eq!(self.message.len(), 0);
        // self.message.clear();
        self.message.reserve(len as usize);
        self.message.truncate(len as usize);
        read.read_exact(&mut self.message).await?;

        /// Validate that the message is valid a flatbuffer.
        let _ = root::<Message>(&self.message)?;
        Ok(true)
    }
}
