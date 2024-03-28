use arrow_buffer::Buffer;
use std::io::{BufWriter, Write};

use vortex::array::Array;
use vortex::serde::data::{ArrayData, ColumnData};

use crate::ALIGNMENT;
use vortex_error::VortexResult;
use vortex_flatbuffers::FlatBufferWriter;

use crate::context::IPCContext;
use crate::messages::{IPCChunk, IPCChunkColumn, IPCMessage, IPCSchema};

#[allow(dead_code)]
pub struct StreamWriter<W: Write> {
    write: W,
    ctx: IPCContext,
}

impl<W: Write> StreamWriter<BufWriter<W>> {
    pub fn try_new(write: W, ctx: IPCContext) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufWriter::new(write), ctx)
    }
}

impl<W: Write> StreamWriter<W> {
    pub fn try_new_unbuffered(mut write: W, ctx: IPCContext) -> VortexResult<Self> {
        // Write the IPC context to the stream
        write.write_flatbuffer(&IPCMessage::Context(&ctx), ALIGNMENT)?;
        Ok(Self { write, ctx })
    }

    pub fn write(&mut self, array: &dyn Array) -> VortexResult<()> {
        // First, write a schema message indicating the start of an array.
        self.write
            .write_flatbuffer(&IPCMessage::Schema(IPCSchema(array.dtype())), ALIGNMENT)?;

        // Then we write the array in chunks.
        // TODO(ngates): should we do any chunking ourselves?
        // TODO(ngates): If it's a chunked array, use those chunks. Else write the whole thing.

        // For now, we write a single chunk.
        self.write_chunk(array)?;

        // todo!("write the array to the stream")
        Ok(())
    }

    fn write_chunk(&mut self, _array: &dyn Array) -> VortexResult<()> {
        // A chunk contains the forward byte offsets to each of the columns in the chunk.
        // TODO(ngates): somehow get the flattened columns as ArrayData.
        let data = ArrayData::new(vec![ColumnData::new(vec![Buffer::from_vec(vec![
            1, 2, 3, 4, 5,
        ])])]);

        let mut offset = 0;
        let mut chunk_column_msgs = Vec::with_capacity(data.columns().len());
        let mut chunk_column_offsets = Vec::with_capacity(data.columns().len());
        for column_data in data.columns() {
            chunk_column_offsets.push(offset);

            // Serialize the ChunkColumn message and add its offset.
            let mut vec = Vec::new();
            vec.write_flatbuffer(
                &IPCMessage::ChunkColumn(IPCChunkColumn(&column_data)),
                ALIGNMENT,
            )?;
            chunk_column_msgs.push(vec);

            // We then leave space for the actual buffers of the column.
            let buffer_offsets = column_data.buffer_offsets(ALIGNMENT);
            offset += buffer_offsets.last().unwrap();
        }

        // Now we can construct a Chunk message with the offsets to each column.
        self.write.write_flatbuffer(
            &IPCMessage::Chunk(IPCChunk(&chunk_column_offsets)),
            ALIGNMENT,
        )?;

        // Then write each chunk column chunk message, followed by its buffers.
        for (msg, column_data) in chunk_column_msgs.iter().zip(data.columns()) {
            self.write.write_all(&msg)?;

            let buffer_offsets = column_data.buffer_offsets(ALIGNMENT);
            let mut current_offset = 0;
            for (buffer_end, buffer) in buffer_offsets.iter().skip(1).zip(column_data.buffers()) {
                self.write.write_all(&buffer.as_slice())?;
                current_offset += buffer.len();
                let padding = buffer_end - current_offset;
                self.write.write_all(&vec![0; padding])?;
            }
        }

        // First, convert the array to ArrayData.
        Ok(())
    }
}
