use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;
use pin_project::pin_project;
use vortex::array::primitive::PrimitiveArray;
use vortex::{IntoArray, OwnedArray};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};

use crate::messages::IPCMessage;

type ArrayChunk = OwnedArray;

/// Iterate over many arrays in a single stream.
pub trait IPCReader: Stream<Item = VortexResult<Box<dyn ArrayReader>>> {}

/// Similar to Arrow RecordBatchReader.
pub trait ArrayReader: Stream<Item = VortexResult<OwnedArray>> {
    fn dtype(&self) -> &DType;
}

#[pin_project]
struct MessageArrayReader<M: MessageReader> {
    dtype: DType,
    #[pin]
    message_reader: M,
}

impl<M: MessageReader> ArrayReader for MessageArrayReader<M> {
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

/// Return a new ArrayChunk from a stream of messages.
impl<M: MessageReader> Stream for MessageArrayReader<M> {
    type Item = VortexResult<ArrayChunk>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let msg = match self.project().message_reader.poll_next(cx) {
            Poll::Ready(Some(msg)) => msg,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        // TODO(ngates): how expensive can this get? Should we offload?
        let chunk = msg.and_then(|msg| {
            if let IPCMessage::Chunk(_chunk) = msg {
                Ok(PrimitiveArray::from(vec![0, 1, 2]).into_array())
            } else {
                Err(vortex_err!("expected IPCChunk"))
            }
        });

        Poll::Ready(Some(chunk))
    }
}

// An abstraction for reading Vortex messages that can be implemented for several IO frameworks.
pub trait MessageReader: Stream<Item = VortexResult<IPCMessage<'static>>> {}

///// Compatability with byte streams

/// Wrap a stream of bytes into a `MessageReader`.
pub struct StreamMessageReader<S, B, E>(pub S)
where
    S: Stream<Item = Result<B, E>>,
    B: Into<Buffer>;

impl<S, B, E> MessageReader for StreamMessageReader<S, B, E>
where
    S: Stream<Item = Result<B, E>>,
    B: Into<Buffer>,
{
}

impl<S, B, E> Stream for StreamMessageReader<S, B, E>
where
    S: Stream<Item = Result<B, E>>,
    B: Into<Buffer>,
{
    type Item = VortexResult<IPCMessage<'static>>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
