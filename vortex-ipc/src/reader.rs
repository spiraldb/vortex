use std::io;
use std::io::{BufReader, Read};

use arrow_buffer::Buffer as ArrowBuffer;
use nougat::gat;
use vortex::array::chunked::ChunkedArray;
use vortex::array::composite::VORTEX_COMPOSITE_EXTENSIONS;
use vortex::buffer::Buffer;
use vortex::{Array, ArrayView, IntoArray, SerdeContext, ToArray, ToStatic};
use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::{FlatBufferReader, ReadFlatBuffer};
use vortex_schema::{DType, DTypeSerdeContext};

use crate::flatbuffers::ipc::Message;
use crate::iter::{FallibleLendingIterator, FallibleLendingIteratorඞItem};

#[allow(dead_code)]
pub struct StreamReader<R: Read> {
    read: R,

    pub(crate) ctx: SerdeContext,
    // Optionally take a projection?

    // Use replace to swap the scratch buffer.
    // std::mem::replace
    // We could use a cell to avoid the need for mutable borrow.
    scratch: Vec<u8>,
}

impl<R: Read> StreamReader<BufReader<R>> {
    pub fn try_new(read: R) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufReader::new(read))
    }
}

impl<R: Read> StreamReader<R> {
    pub fn try_new_unbuffered(mut read: R) -> VortexResult<Self> {
        let mut msg_vec = Vec::new();
        let fb_msg = read
            .read_message::<Message>(&mut msg_vec)?
            .ok_or_else(|| vortex_err!(InvalidSerde: "Unexpected EOF reading IPC format"))?;
        let fb_ctx = fb_msg.header_as_context().ok_or_else(
            || vortex_err!(InvalidSerde: "Expected IPC Context as first message in stream"),
        )?;
        let ctx: SerdeContext = fb_ctx.try_into()?;

        Ok(Self {
            read,
            ctx,
            scratch: Vec::with_capacity(1024),
        })
    }

    /// Read a single array from the IPC stream.
    pub fn read_array(&mut self) -> VortexResult<Array> {
        let mut array_reader = self
            .next()?
            .ok_or_else(|| vortex_err!(InvalidSerde: "Unexpected EOF"))?;

        let mut chunks = vec![];
        while let Some(chunk) = array_reader.next()? {
            chunks.push(chunk.into_array().to_static());
        }

        if chunks.len() == 1 {
            Ok(chunks[0].clone())
        } else {
            ChunkedArray::try_new(chunks.into_iter().collect(), array_reader.dtype().clone())
                .map(|chunked| chunked.into_array())
        }
    }
}

/// We implement a lending iterator here so that each StreamArrayChunkReader can be lent as
/// mutable to the caller. This is necessary because we need a mutable handle to the reader.
#[gat]
impl<R: Read> FallibleLendingIterator for StreamReader<R> {
    type Error = VortexError;
    type Item<'next> =  StreamArrayChunkReader<'next, R> where Self: 'next;

    fn next(&mut self) -> Result<Option<StreamArrayChunkReader<'_, R>>, Self::Error> {
        let mut fb_vec = Vec::new();
        let msg = self.read.read_message::<Message>(&mut fb_vec)?;
        if msg.is_none() {
            // End of the stream
            return Ok(None);
        }
        let msg = msg.unwrap();

        // FIXME(ngates): parse the schema?
        let schema = msg
            .header_as_schema()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Expected IPC Schema message"))?;

        // TODO(ngates): construct this from the SerdeContext.
        let dtype_ctx =
            DTypeSerdeContext::new(VORTEX_COMPOSITE_EXTENSIONS.iter().map(|e| e.id()).collect());
        let dtype = DType::read_flatbuffer(
            &dtype_ctx,
            &schema
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        // Figure out how many columns we have and therefore how many buffers there?
        Ok(Some(StreamArrayChunkReader {
            read: &mut self.read,
            ctx: &self.ctx,
            dtype,
            buffers: vec![],
            column_msg_buffer: vec![],
        }))
    }
}

#[allow(dead_code)]
pub struct StreamArrayChunkReader<'a, R: Read> {
    read: &'a mut R,
    ctx: &'a SerdeContext,
    dtype: DType,
    buffers: Vec<Buffer<'a>>,
    column_msg_buffer: Vec<u8>,
}

impl<'a, R: Read> StreamArrayChunkReader<'a, R> {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }
}

#[gat]
impl<'iter, R: Read> FallibleLendingIterator for StreamArrayChunkReader<'iter, R> {
    type Error = VortexError;
    type Item<'next> = ArrayView<'next> where Self: 'next;

    fn next(&mut self) -> Result<Option<ArrayView<'_>>, Self::Error> {
        self.column_msg_buffer.clear();
        let msg = self
            .read
            .read_message::<Message>(&mut self.column_msg_buffer)?;
        if msg.is_none() {
            // End of the stream
            return Ok(None);
        }
        let msg = msg.unwrap();

        let chunk_msg = msg
            .header_as_chunk()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Expected IPC Chunk message"))
            .unwrap();
        let col_array = chunk_msg
            .array()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Chunk column missing Array"))
            .unwrap();

        // Read all the column's buffers
        // TODO(ngates): read into a single buffer, then Arc::clone and slice
        self.buffers.clear();
        let mut offset = 0;
        for buffer in chunk_msg.buffers().unwrap_or_default().iter() {
            let to_kill = buffer.offset() - offset;
            io::copy(&mut self.read.take(to_kill), &mut io::sink()).unwrap();

            let buffer_length = buffer.length();
            let mut bytes = Vec::with_capacity(buffer_length as usize);
            let bytes_read = self
                .read
                .take(buffer.length())
                .read_to_end(&mut bytes)
                .unwrap();
            if bytes_read < buffer_length as usize {
                return Err(vortex_err!(InvalidSerde: "Unexpected EOF reading buffer"));
            }

            let arrow_buffer = ArrowBuffer::from_vec(bytes);
            assert_eq!(arrow_buffer.len(), buffer_length as usize);
            self.buffers.push(Buffer::Owned(arrow_buffer));

            offset = buffer.offset() + buffer.length();
        }

        // Consume any remaining padding after the final buffer.
        let to_kill = chunk_msg.buffer_size() - offset;
        io::copy(&mut self.read.take(to_kill), &mut io::sink()).unwrap();

        let view = ArrayView::try_new(self.ctx, &self.dtype, col_array, self.buffers.as_slice())?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(Some(view))
    }
}
