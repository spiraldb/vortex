use std::io::{BufWriter, Write};

use itertools::Itertools;
use vortex_array2::{ArrayTrait, SerdeContext};
use vortex_error::VortexResult;
use vortex_flatbuffers::FlatBufferWriter;

use crate::messages::{IPCChunk, IPCContext, IPCMessage, IPCSchema};
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

    fn write_batch(&mut self, array: &dyn ArrayTrait) -> VortexResult<()> {
        let data = array.to_array_data();
        let buffer_offsets = data.all_buffer_offsets(ALIGNMENT);

        // Serialize the Chunk message.
        self.write
            .write_message(&IPCMessage::Chunk(IPCChunk(&self.ctx, &data)), ALIGNMENT)?;

        // Keep track of the offset to add padding after each buffer.
        let mut current_offset = 0;
        for (buffer, &buffer_end) in data
            .depth_first_traversal()
            .flat_map(|data| data.buffers().iter())
            .zip_eq(buffer_offsets.iter().skip(1))
        {
            self.write.write_all(buffer.as_slice())?;
            current_offset += buffer.len();
            let padding = (buffer_end as usize) - current_offset;
            self.write.write_all(&vec![0; padding])?;
        }

        Ok(())
    }
}
