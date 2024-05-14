use std::sync::Arc;

use pin_project::pin_project;
use vortex::{Context, ViewContext};
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};

use crate::codecs::array_reader::{ArrayReader, MessageArrayReader};
use crate::codecs::message_reader::ext::MessageReaderExt;
use crate::codecs::message_reader::MessageReader;

/// An IPC reader is used to emit arrays from a stream of Vortex IPC messages.
#[pin_project]
pub struct IPCReader<'m, M> {
    view_ctx: Arc<ViewContext>,
    messages: &'m mut M,
}

impl<'m, M: MessageReader> IPCReader<'m, M> {
    /// Construct an IPC reader using an existing ViewContext.
    pub fn new(view_ctx: Arc<ViewContext>, messages: &'m mut M) -> Self {
        Self { view_ctx, messages }
    }

    /// Read a ViewContext message from the stream and use it to construct an IPCReader.
    pub async fn try_from_messages(ctx: &Context, messages: &'m mut M) -> VortexResult<Self> {
        let view_ctx = messages.read_view_context(ctx).await?;

        Ok(Self {
            messages,
            view_ctx: Arc::new(view_ctx),
        })
    }

    pub async fn next<'a>(&'a mut self) -> VortexResult<Option<impl ArrayReader + 'a>> {
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

        Ok(Some(
            MessageArrayReader::new(self.view_ctx.clone(), dtype, self.messages).into_reader(),
        ))
    }
}
