use std::io;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use flatbuffers::{root, root_unchecked};

use vortex::{Array, ArrayView, Context, IntoArray};
use vortex::iter::ArrayIterator;
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::flatbuffers::serde as fb;
use crate::io::VortexSyncRead;

pub struct SyncMessageReader<R> {
    read: R,
    message: BytesMut,
    prev_message: BytesMut,
    finished: bool,
}

pub enum ReadState<T> {
    Init,
    ReadMore(u32),
    Finished(T),
}

pub struct DTypeReader {
    state: ReadState<DType>,
    bytes_len: Option<u32>,
}

impl DTypeReader {
    pub fn new() -> Self {
        Self {
            state: ReadState::Init,
            bytes_len: None,
        }
    }

    pub fn read(&mut self, mut buffer: BytesMut) -> VortexResult<ReadState<DType>> {
        match self.state {
            ReadState::Init => Ok(ReadState::ReadMore(4)),
            ReadState::ReadMore(len) => {
                if len as usize != buffer.len() {
                    vortex_bail!("Expected to receive {len} bytes but got {}", buffer.len());
                }

                if self.bytes_len.is_some() {
                } else {
                    let bytes_to_read = buffer.get_u32_le();
                    self.bytes_len = Some(bytes_to_read);
                    Ok(ReadState::ReadMore(bytes_to_read))
                }
            }
            ReadState::Finished(_) => {}
        }
    }
}

impl<R: VortexSyncRead> SyncMessageReader<R> {
    pub fn try_new(read: R) -> VortexResult<Self> {
        let mut reader = Self {
            read,
            message: BytesMut::new(),
            prev_message: BytesMut::new(),
            finished: false,
        };
        reader.load_next_message()?;
        Ok(reader)
    }

    fn load_next_message(&mut self) -> VortexResult<bool> {
        let mut buffer = std::mem::take(&mut self.message);
        buffer.resize(4, 0);
        let mut buffer = match self.read.read_into(buffer) {
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
        self.message = self.read.read_into(buffer)?;

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

    fn next(&mut self) -> VortexResult<fb::Message> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        self.prev_message = self.message.split();
        if !self.load_next_message()? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<fb::Message>(&self.prev_message) })
    }

    fn next_raw(&mut self) -> VortexResult<Buffer> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        self.prev_message = self.message.split();
        if !self.load_next_message()? {
            self.finished = true;
        }
        Ok(Buffer::from(self.prev_message.clone().freeze()))
    }

    /// Fetch the buffers associated with this message.
    fn read_buffers(&mut self) -> VortexResult<Vec<Buffer>> {
        let Some(chunk_msg) = self.peek().and_then(|m| m.header_as_batch()) else {
            // We could return an error here?
            return Ok(Vec::new());
        };

        // Issue a single read to grab all buffers
        let all_buffers_size = chunk_msg.buffer_size();
        let mut all_buffers = BytesMut::with_capacity(all_buffers_size as usize);
        unsafe { all_buffers.set_len(all_buffers_size as usize) };
        let mut all_buffers = self.read.read_into(all_buffers)?;

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
            .collect::<Vec<_>>();

        Ok(buffers)
    }

    pub fn read_dtype(&mut self) -> VortexResult<DType> {
        if self.peek().and_then(|m| m.header_as_schema()).is_none() {
            vortex_bail!("Expected schema message")
        }

        let schema_msg = self.next()?.header_as_schema().unwrap();

        let dtype = DType::try_from(
            schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(dtype)
    }

    pub fn maybe_read_chunk(
        &mut self,
        ctx: Arc<Context>,
        dtype: DType,
    ) -> VortexResult<Option<Array>> {
        let length = match self.peek().and_then(|m| m.header_as_batch()) {
            None => return Ok(None),
            Some(chunk) => chunk.length() as usize,
        };

        let buffers = self.read_buffers()?;
        let flatbuffer = self.next_raw()?;

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
        let array = view.into_array();
        array.with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(Some(array))
    }

    /// Construct an ArrayStream pulling the DType from the stream.
    pub fn array_stream_from_messages(
        &mut self,
        ctx: Arc<Context>,
    ) -> VortexResult<impl ArrayIterator + '_> {
        let dtype = self.read_dtype()?;
        Ok(self.array_stream(ctx, dtype))
    }

    pub fn array_stream(&mut self, ctx: Arc<Context>, dtype: DType) -> impl ArrayIterator + '_ {
        ArrayMessageIter::new(self, ctx, dtype)
    }

    pub fn maybe_read_page(&mut self) -> VortexResult<Option<Buffer>> {
        let Some(page_msg) = self.peek().and_then(|m| m.header_as_page()) else {
            return Ok(None);
        };

        let buffer_len = page_msg.buffer_size() as usize;
        let total_len = buffer_len + (page_msg.padding() as usize);

        let mut buffer = BytesMut::with_capacity(total_len);
        unsafe { buffer.set_len(total_len) }
        buffer = self.read.read_into(buffer)?;
        buffer.truncate(buffer_len);
        let page_buffer = Ok(Some(Buffer::from(buffer.freeze())));
        let _ = self.next()?;
        page_buffer
    }
}

pub struct ArrayMessageIter<'a, R: VortexSyncRead> {
    msgs: &'a mut SyncMessageReader<R>,
    ctx: Arc<Context>,
    dtype: DType,
}

impl<'a, R: VortexSyncRead> ArrayMessageIter<'a, R> {
    pub fn new(msgs: &'a mut SyncMessageReader<R>, ctx: Arc<Context>, dtype: DType) -> Self {
        Self { msgs, ctx, dtype }
    }
}

impl<'a, R: VortexSyncRead> Iterator for ArrayMessageIter<'a, R> {
    type Item = VortexResult<Array>;

    fn next(&mut self) -> Option<Self::Item> {
        self.msgs
            .maybe_read_chunk(self.ctx.clone(), self.dtype.clone())
            .transpose()
    }
}

impl<'a, R: VortexSyncRead> ArrayIterator for ArrayMessageIter<'a, R> {
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}
