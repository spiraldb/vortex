use std::pin::Pin;
use std::task::Poll;

use bytes::BytesMut;
use futures_util::Stream;
use vortex::{ArrayView, Context, IntoArray, OwnedArray, ToArray, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::codecs::message_reader::MessageReader;
use crate::messages::SerdeContextDeserializer;
use crate::reader2::ArrayReader;

pub struct IPCReader<'a, M: MessageReader> {
    view_ctx: ViewContext,
    messages: &'a mut M,
}

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

impl<M: MessageReader> MessageArrayReader<'_, M> {
    /// Construct an ArrayReader with a message stream containing chunk messages.
    pub fn new(ctx: ViewContext, dtype: DType, messages: &mut M) -> Self {
        Self {
            ctx,
            dtype,
            messages,
            buffers: Vec::new(),
            row_offset: 0,
        }
    }

    async fn next_chunk(&mut self) -> VortexResult<Option<OwnedArray>> {
        let Some(chunk_msg) = self.messages.peek().and_then(|msg| msg.header_as_chunk()) else {
            return Ok(None);
        };

        // Read all the column's buffers
        self.buffers.clear();
        let mut offset = 0;
        for buffer in chunk_msg.buffers().unwrap_or_default().iter() {
            let _skip = buffer.offset() - offset;
            self.messages.skip(buffer.offset() - offset).await?;

            // TODO(ngates): read into a single buffer, then Arc::clone and slice
            let bytes = BytesMut::zeroed(buffer.length() as usize);
            let bytes = self.messages.read_into(bytes).await?;
            self.buffers.push(Buffer::from(bytes.freeze()));

            offset = buffer.offset() + buffer.length();
        }

        // Consume any remaining padding after the final buffer.
        self.messages.skip(chunk_msg.buffer_size() - offset).await?;

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

pub struct ArrayReaderImpl {
    dtype: DType,
}

impl ArrayReader for ArrayReaderImpl {
    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl Stream for ArrayReaderImpl {
    type Item = VortexResult<OwnedArray>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}
