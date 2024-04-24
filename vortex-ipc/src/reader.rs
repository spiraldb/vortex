use std::io;
use std::io::{BufReader, Read};

use arrow_buffer::Buffer as ArrowBuffer;
use flatbuffers::{root, root_unchecked};
use nougat::gat;
use vortex::array::chunked::ChunkedArray;
use vortex::array::composite::VORTEX_COMPOSITE_EXTENSIONS;
use vortex::buffer::Buffer;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{Array, ArrayView, IntoArray, OwnedArray, SerdeContext, ToArray, ToStatic};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;
use vortex_schema::{DType, DTypeSerdeContext};

use crate::flatbuffers::ipc::{Message, MessageHeader};
use crate::iter::{FallibleLendingIterator, FallibleLendingIteratorඞItem};

#[allow(dead_code)]
pub struct StreamReader<R: Read> {
    read: R,
    messages: StreamMessageReader,
    ctx: SerdeContext,
}

impl<R: Read> StreamReader<BufReader<R>> {
    pub fn try_new(read: R) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufReader::new(read))
    }
}

impl<R: Read> StreamReader<R> {
    pub fn try_new_unbuffered(mut read: R) -> VortexResult<Self> {
        let mut messages = StreamMessageReader::new();
        if !messages.load_next_message(&mut read)? {
            vortex_bail!("Unexpected EOF reading IPC format");
        }
        let fb_msg = root::<Message>(messages.message())?;
        let fb_ctx = fb_msg.header_as_context().ok_or_else(
            || vortex_err!(InvalidSerde: "Expected IPC Context as first message in stream"),
        )?;
        let ctx: SerdeContext = fb_ctx.try_into()?;

        Ok(Self {
            read,
            messages,
            ctx,
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
        if !self.messages.load_next_message(self.read.by_ref())? {
            return Ok(None);
        }

        let Some(schema_msg) = root::<Message>(self.messages.message())?.header_as_schema() else {
            self.messages.put_back();
            return Ok(None);
        };
        // TODO(ngates): construct this from the SerdeContext.
        let dtype_ctx =
            DTypeSerdeContext::new(VORTEX_COMPOSITE_EXTENSIONS.iter().map(|e| e.id()).collect());
        let dtype = DType::read_flatbuffer(
            &dtype_ctx,
            &schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(Some(StreamArrayReader {
            ctx: &self.ctx,
            read: &mut self.read,
            messages: &mut self.messages,
            dtype,
            buffers: vec![],
        }))
    }
}

#[allow(dead_code)]
pub struct StreamArrayReader<'a, R: Read> {
    ctx: &'a SerdeContext,
    read: &'a mut R,
    messages: &'a mut StreamMessageReader,
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
        if !self.messages.load_next_message(&mut self.read)? {
            return Ok(None);
        }
        if root::<Message>(self.messages.message())?.header_type() != MessageHeader::Chunk {
            self.messages.put_back();
            return Ok(None);
        }
        let chunk_msg = unsafe { root_unchecked::<Message>(self.messages.message()) }
            .header_as_chunk()
            .unwrap();
        let col_array = chunk_msg
            .array()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Chunk column missing Array"))
            .unwrap();

        // Read all the column's buffers
        self.buffers.clear();
        let mut offset = 0;
        for buffer in chunk_msg.buffers().unwrap_or_default().iter() {
            self.read.skip(buffer.offset() - offset)?;

            // TODO(ngates): read into a single buffer, then Arc::clone and slice
            let mut bytes = Vec::with_capacity(buffer.length() as usize);
            self.read.read_into(buffer.length(), &mut bytes)?;
            let arrow_buffer = ArrowBuffer::from_vec(bytes);
            self.buffers.push(Buffer::Owned(arrow_buffer));

            offset = buffer.offset() + buffer.length();
        }

        // Consume any remaining padding after the final buffer.
        self.read.skip(chunk_msg.buffer_size() - offset)?;

        let view = ArrayView::try_new(&self.ctx, &self.dtype, col_array, self.buffers.as_slice())?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(Some(view.into_array()))
    }
}

pub trait ReadExtensions: Read {
    /// Skip n bytes in the stream.
    fn skip(&mut self, nbytes: u64) -> io::Result<()> {
        io::copy(&mut self.take(nbytes), &mut io::sink())?;
        Ok(())
    }

    /// Read exactly nbytes into the buffer.
    fn read_into(&mut self, nbytes: u64, buffer: &mut Vec<u8>) -> io::Result<()> {
        buffer.reserve_exact(nbytes as usize);
        unsafe { buffer.set_len(nbytes as usize) };
        self.read_exact(buffer.as_mut_slice())
    }
}

impl<R: Read> ReadExtensions for R {}

struct StreamMessageReader {
    message: Vec<u8>,
    peeked: bool,
    finished: bool,
}

impl StreamMessageReader {
    pub fn new() -> Self {
        Self {
            message: Vec::new(),
            peeked: false,
            finished: false,
        }
    }

    pub fn message(&self) -> &[u8] {
        &self.message
    }

    pub fn put_back(&mut self) {
        self.peeked = true;
    }

    pub fn load_next_message<R: Read>(&mut self, read: &mut R) -> io::Result<bool> {
        if self.finished {
            return Ok(false);
        }

        if self.peeked {
            self.peeked = false;
            return Ok(true);
        }

        let mut len_buf = [0u8; 4];
        match read.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) => {
                return match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e),
                }
            }
        }

        let len = u32::from_le_bytes(len_buf);
        if len == u32::MAX {
            // Marker for no more messages.
            self.finished = true;
            return Ok(false);
        }

        self.message.clear();
        self.message.reserve(len as usize);
        unsafe { self.message.set_len(len as usize) };
        read.read_exact(self.message.as_mut_slice())?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use vortex::array::chunked::{Chunked, ChunkedArray};
    use vortex::array::primitive::{Primitive, PrimitiveArray};
    use vortex::{ArrayDType, ArrayDef, IntoArray, SerdeContext};

    use crate::reader::StreamReader;
    use crate::writer::StreamWriter;

    #[test]
    fn test_read_write() {
        let array = PrimitiveArray::from(vec![0, 1, 2]).into_array();
        let chunked_array =
            ChunkedArray::try_new(vec![array.clone(), array.clone()], array.dtype().clone())
                .unwrap()
                .into_array();

        let mut buffer = vec![];
        let mut cursor = Cursor::new(&mut buffer);
        {
            let mut writer = StreamWriter::try_new(&mut cursor, SerdeContext::default()).unwrap();
            writer.write_array(&array).unwrap();
            writer.write_array(&chunked_array).unwrap();
        }
        // Push some extra bytes to test that the reader is well-behaved and doesn't read past the
        // end of the stream.
        cursor.write(b"hello").unwrap();

        cursor.set_position(0);
        {
            let mut reader = StreamReader::try_new_unbuffered(&mut cursor).unwrap();
            let first = reader.read_array().unwrap();
            assert_eq!(first.encoding().id(), Primitive::ID);
            let second = reader.read_array().unwrap();
            assert_eq!(second.encoding().id(), Chunked::ID);
        }
        let _pos = cursor.position();
        // Test our termination bytes exist
        let mut terminator = [0u8; 5];
        cursor.read_exact(&mut terminator).unwrap();
        assert_eq!(&terminator, b"hello");
    }
}
