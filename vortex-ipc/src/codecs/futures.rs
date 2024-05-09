#![cfg(feature = "futures")]

use std::pin::Pin;
use std::task::{Context, Poll};

use asynchronous_codec::{Decoder, FramedRead};
use bytes::{Buf, BytesMut};
use futures_util::{AsyncRead, Stream};
use pin_project::pin_project;
use vortex_buffer::Buffer;
use vortex_error::{VortexError, VortexResult};

use crate::codecs::message_stream::MessageStream;

/// The Vortex message codec implemented over streams of bytes.
struct StreamMessageCodec;

impl Decoder for StreamMessageCodec {
    type Item = Buffer;
    type Error = VortexError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        // Extract the length of the message.
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[..4]);
        let len = u32::from_le_bytes(len_bytes) as usize;

        if src.len() - 4 >= len {
            // Skip the length header we already read.
            src.advance(4);
            Ok(Some(src.split_to(len).freeze().into()))
        } else {
            Ok(None)
        }
    }
}

#[pin_project]
pub struct AsyncReadMessageStream<R: AsyncRead + Unpin> {
    #[pin]
    framed: FramedRead<R, StreamMessageCodec>,
}

impl<R: AsyncRead + Unpin> AsyncReadMessageStream<R> {
    pub fn new(read: R) -> Self {
        Self {
            framed: FramedRead::new(read, StreamMessageCodec),
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for AsyncReadMessageStream<R> {
    type Item = VortexResult<Buffer>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Stream::poll_next(self.project().framed, cx)
    }
}
