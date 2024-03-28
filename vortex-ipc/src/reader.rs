use crate::array::ArrayView;
use crate::context::IPCContext;
use crate::flatbuffers::ipc::Message;
use arrow_buffer::Buffer;
use flatbuffers::root;
use lending_iterator::prelude::*;
use std::io;
use std::io::{BufReader, Read};
use vortex_error::{VortexError, VortexResult};
use vortex_flatbuffers::FlatBufferReader;
use vortex_schema::DType;
use vortex_schema::IntWidth::_32;
use vortex_schema::Nullability::Nullable;
use vortex_schema::Signedness::Signed;

#[allow(dead_code)]
pub struct StreamReader<R: Read> {
    read: R,

    pub(crate) ctx: IPCContext,
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
            .read_flatbuffer::<Message>(&mut msg_vec)?
            .ok_or_else(|| VortexError::InvalidSerde("Unexpected EOF reading IPC format".into()))?;
        let fb_ctx = fb_msg.header_as_context().ok_or_else(|| {
            VortexError::InvalidSerde("Expected IPC Context as first message in stream".into())
        })?;
        let ctx: IPCContext = fb_ctx.try_into()?;

        Ok(Self {
            read,
            ctx,
            scratch: Vec::with_capacity(1024),
        })
    }
    //
    // pub fn with_next_array<T>(
    //     mut self,
    //     f: impl FnOnce(&mut dyn ArrayChunkReader) -> VortexResult<T>,
    // ) -> VortexResult<Option<T>> {
    //     let mut array_chunk_reader = StreamArrayChunkReader {
    //         reader: self,
    //         dtype: DType::Int(_32, Signed, Nullable),
    //     };
    //     Ok(Some(f(&mut array_chunk_reader)?))
    // }

    // pub fn into_iter<'a>(self) -> StreamReaderStreamingIterator<'a, R> {
    //     StreamReaderStreamingIterator {
    //         reader: self,
    //         chunk_reader: None,
    //     }
    // }
}

/// We implement a lending iterator here so that each StreamArrayChunkReader can be lent as
/// mutable to the caller. This is necessary because we need a mutable handle to the reader.
#[gat]
impl<R: Read> LendingIterator for StreamReader<R> {
    type Item<'next> = VortexResult<StreamArrayChunkReader<'next, R>> where Self: 'next;

    fn next(self: &'_ mut Self) -> Option<Item<'_, Self>> {
        let mut fb_vec = Vec::new();
        let msg = self
            .read
            .read_flatbuffer::<Message>(&mut fb_vec)
            .transpose();
        if msg.is_none() {
            // End of the stream
            return None;
        }
        let msg = msg.unwrap();

        // Invoke a closure that returns VortexResult<Option<Item>> so we can nicely transpose.
        (move || {
            let _schema = msg?
                .header_as_schema()
                .ok_or_else(|| VortexError::InvalidSerde("Expected IPC Schema message".into()))?;
            Ok(Some(StreamArrayChunkReader {
                read: &mut self.read,
                ctx: &self.ctx,
                dtype: DType::Int(_32, Signed, Nullable),
                fb_buffer: Vec::new(),
                buffers: Vec::new(),
            }))
        })()
        .transpose()
    }
}

#[allow(dead_code)]
pub struct StreamArrayChunkReader<'a, R: Read> {
    read: &'a mut R,
    ctx: &'a IPCContext,
    dtype: DType,
    fb_buffer: Vec<u8>,
    buffers: Vec<Buffer>,
}

impl<'a, R: Read> StreamArrayChunkReader<'a, R> {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }
}

#[gat]
impl<'a, R: Read> LendingIterator for StreamArrayChunkReader<'a, R> {
    type Item<'next> = VortexResult<ArrayView<'next>> where Self: 'next;

    fn next(self: &'_ mut Self) -> Option<Item<'_, Self>> {
        let mut fb_vec: Vec<u8> = Vec::new();
        let msg = self
            .read
            .read_flatbuffer::<Message>(&mut fb_vec)
            .transpose();
        if msg.is_none() {
            // End of the stream
            return None;
        }
        let msg = msg.unwrap();

        let msg = msg.unwrap();
        let chunk = msg
            .header_as_chunk()
            .ok_or_else(|| VortexError::InvalidSerde("Expected IPC Chunk message".into()))
            .unwrap();

        let col_offsets = chunk
            .column_offsets()
            .ok_or_else(|| {
                VortexError::InvalidSerde("Expected column offsets in IPC Chunk message".into())
            })
            .unwrap();
        assert_eq!(col_offsets.len(), 1);

        // TODO(ngates): read each column
        read_into(self.read, &mut self.fb_buffer).unwrap();
        let col_msg = root::<Message>(&self.fb_buffer)
            .unwrap()
            .header_as_chunk_column()
            .ok_or_else(|| VortexError::InvalidSerde("Expected IPC Chunk Column message".into()))
            .unwrap();

        let col_array = col_msg
            .array()
            .ok_or_else(|| VortexError::InvalidSerde("Chunk column missing Array".into()))
            .unwrap();

        // Read all the column's buffers
        self.buffers.clear();
        let mut offset = 0;
        for (buffer_offset, buffer_def) in col_msg
            .buffer_offsets()
            .unwrap_or_default()
            .iter()
            .zip(col_array.buffers().unwrap_or_default().iter())
        {
            let to_kill = buffer_offset - offset;
            io::copy(&mut self.read.take(to_kill), &mut io::sink()).unwrap();

            let mut buffer = vec![0u8; buffer_def.length() as usize];
            self.read.read_exact(&mut buffer).unwrap();
            self.buffers.push(Buffer::from_vec(buffer));

            offset = buffer_offset + buffer_def.length();
        }

        // Consume any remaining padding after the final buffer.
        col_msg
            .buffer_offsets()
            .unwrap()
            .iter()
            .last()
            .map(|last_offset| {
                let to_kill = last_offset - offset;
                io::copy(&mut self.read.take(to_kill), &mut io::sink()).unwrap();
            });

        Some(Ok(ArrayView {
            ctx: self.ctx,
            array: col_array,
            fb_buffer: &self.fb_buffer,
            buffers: &self.buffers,
        }))
    }
}

pub fn read_into<R: Read>(read: &mut R, buffer: &mut Vec<u8>) -> VortexResult<()> {
    buffer.clear();

    let mut buffer_len: [u8; 4] = [0; 4];
    // FIXME(ngates): return optional for EOF?
    read.read_exact(&mut buffer_len)?;

    let buffer_len = u32::from_le_bytes(buffer_len) as usize;
    read.take(buffer_len as u64).read_to_end(buffer)?;

    Ok(())
}

// impl<R: Read> ArrayViewChunkReader for StreamArrayChunkReader<'_, R> {
//     fn dtype(&self) -> &DType {
//         &self.dtype
//     }
// }
