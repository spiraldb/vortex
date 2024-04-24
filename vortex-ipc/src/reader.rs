use std::io;
use std::io::{BufReader, Read};

use arrow_buffer::Buffer as ArrowBuffer;
use flatbuffers::root;
use nougat::gat;
use vortex::array::chunked::ChunkedArray;
use vortex::array::composite::VORTEX_COMPOSITE_EXTENSIONS;
use vortex::buffer::Buffer;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{Array, ArrayView, IntoArray, OwnedArray, SerdeContext, ToArray, ToStatic};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;
use vortex_schema::{DType, DTypeSerdeContext};

use crate::flatbuffers::ipc::Message;
use crate::iter::{FallibleLendingIterator, FallibleLendingIteratorඞItem};

#[allow(dead_code)]
pub struct StreamReader<R: Read> {
    read: R,
    ctx: SerdeContext,
    next_message: Vec<u8>,
    // Flag set when we encounter the stream termination marker.
    finished: bool,
}

impl<R: Read> StreamReader<BufReader<R>> {
    pub fn try_new(read: R) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufReader::new(read))
    }
}

impl<R: Read> StreamReader<R> {
    pub fn try_new_unbuffered(mut read: R) -> VortexResult<Self> {
        let mut next_message = Vec::new();
        if !read.read_length_prefixed(&mut next_message)? {
            return Err(vortex_err!(InvalidSerde: "Unexpected EOF reading IPC format"));
        }
        let fb_msg = root::<Message>(&next_message)?;
        let fb_ctx = fb_msg.header_as_context().ok_or_else(
            || vortex_err!(InvalidSerde: "Expected IPC Context as first message in stream"),
        )?;
        let ctx: SerdeContext = fb_ctx.try_into()?;

        Ok(Self {
            read,
            ctx,
            next_message,
            finished: false,
        })
    }

    /// Read a single array from the IPC stream.
    pub fn read_array(&mut self) -> VortexResult<Array> {
        let mut array_reader = self
            .next()?
            .ok_or_else(|| vortex_err!(InvalidSerde: "Unexpected EOF"))?;

        let mut chunks = vec![];
        while let Some(chunk) = array_reader.next()? {
            chunks.push(chunk.to_static());
        }

        if chunks.len() == 1 {
            Ok(chunks[0].clone())
        } else {
            ChunkedArray::try_new(chunks.into_iter().collect(), array_reader.dtype().clone())
                .map(|chunked| chunked.into_array())
        }
    }
}

#[gat]
impl<R: Read> FallibleLendingIterator for StreamReader<R> {
    type Error = VortexError;
    type Item<'next> =  StreamArrayReader<'next, R> where Self: 'next;

    fn next(&mut self) -> Result<Option<StreamArrayReader<'_, R>>, Self::Error> {
        if self.finished || !self.read.read_length_prefixed(&mut self.next_message)? {
            // End of stream
            self.finished = true;
            return Ok(None);
        }

        let msg = root::<Message>(&self.next_message)?;
        let schema = match msg.header_as_schema() {
            None => return Ok(None),
            Some(header) => header,
        };
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

        Ok(Some(StreamArrayReader {
            ctx: &self.ctx,
            read: &mut self.read,
            next_message: &mut self.next_message,
            finished: &mut self.finished,
            dtype,
            buffers: vec![],
        }))
    }
}

#[allow(dead_code)]
pub struct StreamArrayReader<'a, R: Read> {
    ctx: &'a SerdeContext,
    read: &'a mut R,
    next_message: &'a mut Vec<u8>,
    finished: &'a mut bool,
    dtype: DType,
    buffers: Vec<Buffer<'a>>,
}

impl<'a, R: Read> StreamArrayReader<'a, R> {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn take(&self, indices: &Array<'_>) -> VortexResult<OwnedArray> {
        if !indices
            .statistics()
            .compute_as::<bool>(Stat::IsSorted)
            .unwrap_or_default()
        {
            vortex_bail!("Indices must be sorted to take from IPC stream")
        }
        todo!()
    }
}

#[gat]
impl<'iter, R: Read> FallibleLendingIterator for StreamArrayReader<'iter, R> {
    type Error = VortexError;
    type Item<'next> = Array<'next> where Self: 'next;

    fn next(&mut self) -> Result<Option<Array<'_>>, Self::Error> {
        if *self.finished || !self.read.read_length_prefixed(&mut self.next_message)? {
            // End of stream
            *self.finished = true;
            return Ok(None);
        }

        let msg = root::<Message>(&self.next_message)?;
        let chunk_msg = match msg.header_as_chunk() {
            None => return Ok(None),
            Some(header) => header,
        };
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

        let view = ArrayView::try_new(&self.ctx, &self.dtype, col_array, self.buffers.as_slice())?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(Some(view.into_array()))
    }
}

pub trait LengthPrefixedReader: Read {
    fn read_length_prefixed(&mut self, buffer: &mut Vec<u8>) -> io::Result<bool> {
        buffer.clear();

        let mut len_buf = [0u8; 4];
        match self.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) => match e.kind() {
                io::ErrorKind::UnexpectedEof => return Ok(false),
                _ => {
                    return Err(e);
                }
            },
        }

        let len = u32::from_le_bytes(len_buf);
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        }

        self.take(len as u64).read_to_end(buffer)?;
        Ok(true)
    }
}

impl<R: Read> LengthPrefixedReader for R {}
