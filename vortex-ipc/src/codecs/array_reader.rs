use std::pin::Pin;
use std::task::Poll;

use futures_util::Stream;
use pin_project::pin_project;
use vortex::{Array, ArrayView, IntoArray, OwnedArray, ToArray, ToStatic, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{VortexError, VortexResult};

use crate::codecs::message_reader::MessageReader;

/// A stream of array chunks along with a DType.
///
/// Can be thought of as equivalent to Arrow's RecordBatchReader.
pub trait ArrayReader: Stream<Item = VortexResult<OwnedArray>> {
    fn dtype(&self) -> &DType;
}

/// An adapter for a stream of array chunks to implement an ArrayReader.
#[pin_project]
struct ArrayReaderAdapter<S> {
    dtype: DType,
    #[pin]
    inner: S,
}

impl<S> ArrayReaderAdapter<S> {
    pub fn new(dtype: DType, inner: S) -> Self {
        Self { dtype, inner }
    }
}

impl<S> ArrayReader for ArrayReaderAdapter<S>
where
    S: Stream<Item = VortexResult<OwnedArray>>,
{
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl<S> Stream for ArrayReaderAdapter<S>
where
    S: Stream<Item = VortexResult<OwnedArray>>,
{
    type Item = VortexResult<OwnedArray>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub(crate) struct MessageArrayReader<'a, M: MessageReader> {
    ctx: ViewContext,
    dtype: DType,
    messages: &'a mut M,

    // State
    buffers: Vec<Buffer>,
    row_offset: usize,
}

impl<'m, M: MessageReader> MessageArrayReader<'m, M> {
    /// Construct an ArrayReader with a message stream containing chunk messages.
    pub fn new(ctx: ViewContext, dtype: DType, messages: &'m mut M) -> Self {
        Self {
            ctx,
            dtype,
            messages,
            buffers: Vec::new(),
            row_offset: 0,
        }
    }

    pub fn into_reader(self) -> impl ArrayReader + 'm {
        let dtype = self.dtype.clone();

        let inner = futures_util::stream::unfold(self, move |mut reader| async move {
            match reader.next().await {
                Ok(Some(array)) => Some((Ok(array.to_static()), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        });

        ArrayReaderAdapter { dtype, inner }
    }
}

impl<M: MessageReader> MessageArrayReader<'_, M> {
    pub async fn next(&mut self) -> VortexResult<Option<Array>> {
        if self
            .messages
            .peek()
            .and_then(|msg| msg.header_as_chunk())
            .is_none()
        {
            return Ok(None);
        }

        // TODO(ngates): can we reuse our existing buffers?
        self.buffers = self.messages.buffers().await?;

        // After reading the buffers we're now able to load the next message.
        let col_array = self
            .messages
            .next()
            .await?
            .header_as_chunk()
            .unwrap()
            .array()
            .unwrap();

        let view = ArrayView::try_new(&self.ctx, &self.dtype, col_array, self.buffers.as_slice())?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        let array = view.into_array();
        self.row_offset += array.len();
        Ok(Some(array))
    }
}
