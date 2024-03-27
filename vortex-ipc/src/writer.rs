use std::io::{BufWriter, Write};

use vortex::array::Array;

use vortex_error::VortexResult;
use vortex_flatbuffers::FlatBufferWriter;

use crate::context::IPCContext;
use crate::Message;

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
        write.write_flatbuffer(&Message::Context(&ctx))?;
        Ok(Self { write, ctx })
    }

    pub fn write(&mut self, array: &dyn Array) -> VortexResult<()> {
        // First, write a schema message indicating the start of an array.
        self.write
            .write_flatbuffer(&Message::Schema(array.dtype()))?;

        // Then we write the array in chunks.
        // TODO(ngates): should we do any chunking ourselves?

        // TODO(ngates): If it's a chunked array, use those chunks. Else write the whole thing.

        // todo!("write the array to the stream")
        Ok(())
    }
}
