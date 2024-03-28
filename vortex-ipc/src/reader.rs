use crate::chunked::ArrayChunkReader;
use crate::context::IPCContext;
use crate::flatbuffers::ipc::Message;
use lending_iterator::prelude::*;
use std::io::{BufReader, Read};
use vortex::array::ArrayRef;
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
                dtype: DType::Int(_32, Signed, Nullable),
            }))
        })()
        .transpose()
    }
}

pub struct StreamArrayChunkReader<'a, R: Read> {
    read: &'a mut R,
    dtype: DType,
}

impl<'a, R: Read> ArrayChunkReader for StreamArrayChunkReader<'a, R> {
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl<'a, R: Read> Iterator for StreamArrayChunkReader<'a, R> {
    type Item = VortexResult<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = Vec::new();
        let _msg = self.read.read_flatbuffer::<Message>(&mut buffer);
        print!("MSG {:?}", _msg);
        None
    }
}
