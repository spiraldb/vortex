use crate::chunked::ArrayViewChunkReader;
use crate::context::IPCContext;
use crate::flatbuffers::ipc::Message;
use lending_iterator::prelude::*;
use std::io::{BufReader, Read};
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
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
                chunk: None,
            }))
        })()
        .transpose()
    }
}

pub struct StreamArrayChunkReader<'a, R: Read> {
    read: &'a mut R,
    ctx: &'a IPCContext,
    dtype: DType,
    chunk: Option<ArrayRef>,
}

#[gat]
impl<'a, R: Read> LendingIterator for StreamArrayChunkReader<'a, R> {
    type Item<'next> = VortexResult<&'next dyn Array> where Self: 'next;

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

        (move || {
            let msg = msg?;
            println!("MESSAGE: {:?}", msg);
            let chunk = msg
                .header_as_chunk()
                .ok_or_else(|| VortexError::InvalidSerde("Expected IPC Chunk message".into()))?;

            let col_offsets = chunk.column_offsets().ok_or_else(|| {
                VortexError::InvalidSerde("Expected column offsets in IPC Chunk message".into())
            })?;

            let mut offset = 0;
            let mut col_vec = Vec::new();
            for col_offset in col_offsets {
                // TODO(ngates): drop bytes until we reach col_offset.
                col_vec.clear();
                let col_msg = self
                    .read
                    .read_flatbuffer::<Message>(&mut col_vec)?
                    .ok_or_else(|| {
                        VortexError::InvalidSerde("Unexpected EOF reading IPC format".into())
                    })?
                    .header_as_chunk_column()
                    .ok_or_else(|| {
                        VortexError::InvalidSerde("Expected IPC Chunk Column message".into())
                    })?;

                let encoding = self.ctx.find_encoding(col_msg.encoding()).ok_or_else(|| {
                    VortexError::InvalidSerde("Unknown encoding in IPC Chunk Column message".into())
                })?;

                println!("Chunk column: {:?} {}", col_msg, encoding);
            }

            // TODO(ngates): select the columns to read from the chunk
            println!("Chunk: {:?}", chunk);

            // TODO(ngates): construct a reference over the array data.
            self.chunk = Some(PrimitiveArray::from(vec![1, 2, 3, 4, 5]).into_array());

            Ok(self.chunk.as_deref())
        })()
        .transpose()
    }
}

impl<R: Read> ArrayViewChunkReader for StreamArrayChunkReader<'_, R> {
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}
