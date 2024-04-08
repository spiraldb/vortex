use std::io::{BufWriter, Write};

use flatbuffers::root_unchecked;
use itertools::Itertools;
use vortex_array2::{ArrayTrait, SerdeContext};
use vortex_error::VortexResult;
use vortex_flatbuffers::FlatBufferWriter;

use crate::flatbuffers::ipc as fb;
use crate::messages::{IPCChunk, IPCChunkColumn, IPCContext, IPCMessage, IPCSchema};
use crate::ALIGNMENT;

#[allow(dead_code)]
pub struct StreamWriter<W: Write> {
    write: W,
    ctx: SerdeContext,
}

impl<W: Write> StreamWriter<BufWriter<W>> {
    pub fn try_new(write: W, ctx: SerdeContext) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufWriter::new(write), ctx)
    }
}

impl<W: Write> StreamWriter<W> {
    pub fn try_new_unbuffered(mut write: W, ctx: SerdeContext) -> VortexResult<Self> {
        // Write the IPC context to the stream
        write.write_message(&IPCMessage::Context(IPCContext(&ctx)), ALIGNMENT)?;
        Ok(Self { write, ctx })
    }

    pub fn write(&mut self, array: &dyn ArrayTrait) -> VortexResult<()> {
        // First, write a schema message indicating the start of an array.
        self.write
            .write_message(&IPCMessage::Schema(IPCSchema(array.dtype())), ALIGNMENT)?;

        // Then we write the array in batchs.
        // TODO(ngates): should we do any batching ourselves?
        // TODO(ngates): If it's a batched array, use those batchs. Else write the whole thing.

        // For now, we write a single batch.
        self.write_batch(array)
    }

    /// NOTE(ngates): Initially I had designed the IPC format to flatten nested fields into
    ///  a column batch. Each column (leaf field) would then be written as its own flatbuffer
    ///  message allowing us to seek between columns without loading the encoding metadata
    ///  for the entire batch. To support this, we'd need a new FlatArrayView type in our
    ///  enum that handled the un-flattening on the other end.
    ///
    ///  In the meantime, I'm just going to write the whole batch as if it is a single column
    ///  since the encoding metadata is actually quite small. e.g. ALPArray is 2 bytes for the
    ///  e and f constants... So this may not even be an issue with ~100k columns.
    fn write_batch(&mut self, array: &dyn ArrayTrait) -> VortexResult<()> {
        // In order to generate batch metadata, we need to know the forward offsets for each
        // column. To compute this, we need to know how big the metadata messages are for each
        // column, as well as how long their buffers are.
        // let ncolumns = batch.ncolumns();
        let ncolumns = 1;
        let mut offset = 0;
        let mut batch_column_msgs = Vec::with_capacity(ncolumns);
        let mut batch_column_offsets = Vec::with_capacity(ncolumns);

        let columns = [array.to_array_data()];
        // for column_data in batch.columns() {
        batch_column_offsets.push(offset);

        // Serialize the ChunkColumn message and add its offset.
        let mut vec = Vec::new();
        vec.write_message(
            &IPCMessage::ChunkColumn(IPCChunkColumn(&self.ctx, &columns[0])),
            ALIGNMENT,
        )?;

        // Parse our message to extract the total size used by all buffers of the column.
        let batch_col = unsafe { root_unchecked::<fb::Message>(&vec[4..]) }
            .header_as_chunk_column()
            .unwrap();
        offset += batch_col.buffer_size();
        println!("Offset {}", offset);

        batch_column_msgs.push(vec);
        // }

        // Now we can construct a Chunk message with the offsets to each column.
        self.write.write_message(
            &IPCMessage::Chunk(IPCChunk(&batch_column_offsets)),
            ALIGNMENT,
        )?;

        // Then write each batch column batch message, followed by its buffers.
        for (msg, column_data) in batch_column_msgs.iter().zip(columns) {
            self.write.write_all(msg)?;

            let buffer_offsets = column_data.all_buffer_offsets(ALIGNMENT);
            let mut current_offset = 0;
            for (buffer, &buffer_end) in column_data
                .depth_first_traversal()
                .flat_map(|data| data.buffers().iter())
                .zip_eq(buffer_offsets.iter().skip(1))
            {
                self.write.write_all(buffer.as_slice())?;
                current_offset += buffer.len();
                let padding = (buffer_end as usize) - current_offset;
                self.write.write_all(&vec![0; padding])?;
            }
        }

        Ok(())
    }
}
