use std::pin::Pin;
use std::task::Poll;

use futures_util::Stream;
use pin_project::pin_project;
use vortex::{Array, ArrayView, Context, IntoArray, OwnedArray, ToArray, ToStatic, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::codecs::message_reader::MessageReader;
use crate::messages::SerdeContextDeserializer;

pub struct IPCReader<'a, M: MessageReader> {
    view_ctx: ViewContext,
    messages: &'a mut M,
}

#[allow(dead_code)]
impl<'m, M: MessageReader> IPCReader<'m, M> {
    pub fn new(view_ctx: ViewContext, messages: &'m mut M) -> Self {
        Self { view_ctx, messages }
    }

    pub async fn try_from_messages(ctx: &Context, messages: &'m mut M) -> VortexResult<Self> {
        match messages.peek() {
            None => vortex_bail!("IPC stream is empty"),
            Some(msg) => {
                if msg.header_as_context().is_none() {
                    vortex_bail!(InvalidSerde: "Expected IPC Context as first message in stream")
                }
            }
        }

        let view_ctx: ViewContext = SerdeContextDeserializer {
            fb: messages.next().await?.header_as_context().unwrap(),
            ctx,
        }
        .try_into()?;

        Ok(Self { messages, view_ctx })
    }

    pub async fn next<'a>(&'a mut self) -> VortexResult<Option<MessageArrayReader<'a, M>>> {
        if self
            .messages
            .peek()
            .and_then(|msg| msg.header_as_schema())
            .is_none()
        {
            return Ok(None);
        }

        let schema_msg = self.messages.next().await?.header_as_schema().unwrap();

        let dtype = DType::try_from(
            schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(Some(MessageArrayReader::new(
            self.view_ctx.clone(),
            dtype,
            &mut self.messages,
        )))
    }
}

pub struct MessageArrayReader<'a, M: MessageReader> {
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

        let inner = futures_util::stream::unfold(self, |mut reader| async move {
            match reader.next().await {
                Ok(Some(array)) => Some((Ok(array.to_static()), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        });

        ArrayReaderImpl { dtype, inner }
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

/// A stream of array chunks along with a DType.
pub trait ArrayReader: Stream<Item = VortexResult<OwnedArray>> {
    #[allow(dead_code)]
    fn dtype(&self) -> &DType;
}

#[pin_project]
struct ArrayReaderImpl<S>
where
    S: Stream<Item = VortexResult<OwnedArray>>,
{
    dtype: DType,
    #[pin]
    inner: S,
}

impl<S> ArrayReader for ArrayReaderImpl<S>
where
    S: Stream<Item = VortexResult<OwnedArray>>,
{
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl<S> Stream for ArrayReaderImpl<S>
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
