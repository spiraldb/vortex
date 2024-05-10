use pin_project::pin_project;
use vortex::{Context, ViewContext};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::codecs::array_reader::MessageArrayReader;
use crate::codecs::message_reader::MessageReader;
use crate::messages::SerdeContextDeserializer;

/// An IPC reader is used to emit arrays from a stream of Vortex IPC messages.
#[pin_project]
pub struct IPCReader<'m, M> {
    view_ctx: ViewContext,
    messages: &'m mut M,
}

impl<'m, M: MessageReader> IPCReader<'m, M> {
    /// Construct an IPC reader using an existing ViewContext.
    pub fn new(view_ctx: ViewContext, messages: &'m mut M) -> Self {
        Self { view_ctx, messages }
    }

    /// Read a ViewContext message from the stream and use it to construct an IPCReader.
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
