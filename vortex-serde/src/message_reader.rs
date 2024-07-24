use std::io;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use flatbuffers::{root, root_unchecked};
use futures_util::stream::try_unfold;
use itertools::Itertools;

use vortex::{Array, ArrayView, Context, IntoArray, ToArray};
use vortex::stream::{ArrayStream, ArrayStreamAdapter};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::flatbuffers::serde as fb;
use crate::io::VortexRead;

pub struct MessageReader<R> {
    read: R,
    message: BytesMut,
    prev_message: BytesMut,
    finished: bool,
}

impl<R: VortexRead> MessageReader<R> {
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
        let mut buffer = std::mem::take(&mut self.message);
        buffer.resize(4, 0);
        let mut buffer = match self.read.read_into(buffer).await {
            Ok(b) => b,
            Err(e) => {
                return match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e.into()),
                };
            }
        };

        let len = buffer.get_u32_le();
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        } else if len == 0 {
            vortex_bail!(InvalidSerde: "Invalid IPC stream")
        }

        buffer.reserve(len as usize);
        unsafe { buffer.set_len(len as usize) };
        self.message = self.read.read_into(buffer).await?;

        // Validate that the message is valid a flatbuffer.
        root::<fb::Message>(&self.message).map_err(
            |e| vortex_err!(InvalidSerde: "Failed to parse flatbuffer message: {:?}", e),
        )?;

        Ok(true)
    }

    fn peek(&self) -> Option<fb::Message> {
        if self.finished {
            return None;
        }
        // The message has been validated by the next() call.
        Some(unsafe { root_unchecked::<fb::Message>(&self.message) })
    }

    async fn next(&mut self) -> VortexResult<fb::Message> {
        if self.finished {
            vortex_bail!("Reader is finished, should've checked peek!")
        }
        self.prev_message = self.message.split();
        if !self.load_next_message().await? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<fb::Message>(&self.prev_message) })
    }

    async fn next_raw(&mut self) -> VortexResult<Buffer> {
        if self.finished {
            vortex_bail!("Reader is finished, should've checked peek!")
        }
        self.prev_message = self.message.split();
        if !self.load_next_message().await? {
            self.finished = true;
        }
        Ok(Buffer::from(self.prev_message.clone().freeze()))
    }

    /// Fetch the buffers associated with this message.
    async fn read_buffers(&mut self) -> VortexResult<Vec<Buffer>> {
        let Some(chunk_msg) = self.peek().and_then(|m| m.header_as_batch()) else {
            // We could return an error here?
            return Ok(Vec::new());
        };

        // Issue a single read to grab all buffers
        let all_buffers_size = chunk_msg.buffer_size();
        let mut all_buffers = BytesMut::with_capacity(all_buffers_size as usize);
        unsafe { all_buffers.set_len(all_buffers_size as usize) };
        let mut all_buffers = self.read.read_into(all_buffers).await?;

        // Split out into individual buffers
        // Initialize the column's buffers for a vectored read.
        // To start with, we include the padding and then truncate the buffers after.
        let ipc_buffers = self
            .peek()
            .expect("Checked above in peek")
            .header_as_batch()
            .expect("Checked above in peek")
            .buffers()
            .unwrap_or_default();
        let buffers = ipc_buffers
            .iter()
            .zip(
                ipc_buffers
                    .iter()
                    .map(|b| b.offset())
                    .skip(1)
                    .chain([all_buffers_size]),
            )
            .map(|(buffer, next_offset)| {
                let len = next_offset - buffer.offset() - buffer.padding() as u64;

                // Grab the buffer
                let data_buffer = all_buffers.split_to(len as usize);
                // Strip off any padding from the previous buffer
                all_buffers.advance(buffer.padding() as usize);

                Buffer::from(data_buffer.freeze())
            })
            .collect_vec();

        Ok(buffers)
    }

    pub async fn read_dtype(&mut self) -> VortexResult<DType> {
        if self.peek().and_then(|m| m.header_as_schema()).is_none() {
            vortex_bail!("Expected schema message")
        }

        let schema_msg = self.next().await?.header_as_schema().unwrap();

        let dtype = DType::try_from(
            schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(dtype)
    }

    pub async fn maybe_read_chunk(
        &mut self,
        ctx: Arc<Context>,
        dtype: DType,
    ) -> VortexResult<Option<Array>> {
        let length = match self.peek().and_then(|m| m.header_as_batch()) {
            None => return Ok(None),
            Some(chunk) => chunk.length() as usize,
        };

        let buffers = self.read_buffers().await?;
        let flatbuffer = self.next_raw().await?;

        let view = ArrayView::try_new(
            ctx,
            dtype,
            length,
            flatbuffer,
            |flatbuffer| {
                unsafe { root_unchecked::<fb::Message>(flatbuffer) }
                    .header_as_batch()
                    .unwrap()
                    .array()
                    .ok_or_else(|| vortex_err!("Chunk missing Array"))
            },
            buffers,
        )?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(Some(view.into_array()))
    }

    /// Construct an ArrayStream pulling the DType from the stream.
    pub async fn array_stream_from_messages(
        &mut self,
        ctx: Arc<Context>,
    ) -> VortexResult<impl ArrayStream + '_> {
        let dtype = self.read_dtype().await?;
        Ok(self.array_stream(ctx, dtype))
    }

    pub fn array_stream(&mut self, ctx: Arc<Context>, dtype: DType) -> impl ArrayStream + '_ {
        struct State<'a, R: VortexRead> {
            msgs: &'a mut MessageReader<R>,
            ctx: Arc<Context>,
            dtype: DType,
        }

        let init = State {
            msgs: self,
            ctx,
            dtype: dtype.clone(),
        };

        ArrayStreamAdapter::new(
            dtype,
            try_unfold(init, |state| async move {
                match state
                    .msgs
                    .maybe_read_chunk(state.ctx.clone(), state.dtype.clone())
                    .await?
                {
                    None => Ok(None),
                    Some(array) => Ok(Some((array, state))),
                }
            }),
        )
    }

    pub fn into_array_stream(self, ctx: Arc<Context>, dtype: DType) -> impl ArrayStream {
        struct State<R: VortexRead> {
            msgs: MessageReader<R>,
            ctx: Arc<Context>,
            dtype: DType,
        }

        let init = State {
            msgs: self,
            ctx,
            dtype: dtype.clone(),
        };

        ArrayStreamAdapter::new(
            dtype,
            try_unfold(init, |mut state| async move {
                match state
                    .msgs
                    .maybe_read_chunk(state.ctx.clone(), state.dtype.clone())
                    .await?
                {
                    None => Ok(None),
                    Some(array) => Ok(Some((array, state))),
                }
            }),
        )
    }

    pub async fn maybe_read_page(&mut self) -> VortexResult<Option<Buffer>> {
        let Some(page_msg) = self.peek().and_then(|m| m.header_as_page()) else {
            return Ok(None);
        };

        let buffer_len = page_msg.buffer_size() as usize;
        let total_len = buffer_len + (page_msg.padding() as usize);

        let mut buffer = BytesMut::with_capacity(total_len);
        unsafe { buffer.set_len(total_len) }
        buffer = self.read.read_into(buffer).await?;
        buffer.truncate(buffer_len);
        let page_buffer = Ok(Some(Buffer::from(buffer.freeze())));
        let _ = self.next().await?;
        page_buffer
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use bytes::Bytes;
    use futures_executor::block_on;

    use vortex_buffer::Buffer;

    use crate::{MessageReader, MessageWriter};

    #[test]
    fn read_write_page() {
        let write = Vec::new();
        let mut writer = MessageWriter::new(write);
        block_on(async {
            writer
                .write_page(Buffer::Bytes(Bytes::from("somevalue")))
                .await
        })
        .unwrap();
        let written = writer.into_inner();
        let mut reader =
            block_on(async { MessageReader::try_new(Cursor::new(written.as_slice())).await })
                .unwrap();
        let read_page = block_on(async { reader.maybe_read_page().await })
            .unwrap()
            .unwrap();
        assert_eq!(read_page, Buffer::Bytes(Bytes::from("somevalue")));
    }
}
