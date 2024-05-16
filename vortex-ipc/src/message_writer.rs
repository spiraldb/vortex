use std::io;

use flatbuffers::FlatBufferBuilder;
use itertools::Itertools;
use vortex::{ArrayData, ViewContext};
use vortex_buffer::io::IoBuf;
use vortex_dtype::DType;
use vortex_flatbuffers::WriteFlatBuffer;

use crate::io::VortexWrite;
use crate::messages::{IPCChunk, IPCContext, IPCMessage, IPCSchema};
use crate::ALIGNMENT;

const ZEROS: [u8; 512] = [0u8; 512];

#[derive(Debug)]
pub struct MessageWriter<W> {
    write: W,
    pos: u64,
    alignment: usize,

    scratch: Option<Vec<u8>>,
}

impl<W: VortexWrite> MessageWriter<W> {
    pub fn new(write: W) -> Self {
        assert!(ALIGNMENT <= ZEROS.len(), "ALIGNMENT must be <= 512");
        Self {
            write,
            pos: 0,
            alignment: ALIGNMENT,
            scratch: Some(Vec::new()),
        }
    }

    pub fn into_write(self) -> W {
        self.write
    }

    /// Returns the current position in the stream.
    pub fn tell(&self) -> u64 {
        self.pos
    }

    pub async fn write_view_context(&mut self, view_ctx: &ViewContext) -> io::Result<()> {
        self.write_message(IPCMessage::Context(IPCContext(&view_ctx)))
            .await
    }

    pub async fn write_dtype(&mut self, dtype: &DType) -> io::Result<()> {
        self.write_message(IPCMessage::Schema(IPCSchema(dtype)))
            .await
    }

    pub async fn write_chunk(
        &mut self,
        view_ctx: &ViewContext,
        // TODO(ngates): should we support writing from an ArrayView?
        chunk: ArrayData,
    ) -> io::Result<()> {
        let buffer_offsets = chunk.all_buffer_offsets(self.alignment);

        // Serialize the Chunk message.
        self.write_message(IPCMessage::Chunk(IPCChunk(view_ctx, &chunk)))
            .await?;

        // Keep track of the offset to add padding after each buffer.
        let mut current_offset = 0;
        for (buffer, &buffer_end) in chunk
            .depth_first_traversal()
            .flat_map(|data| data.buffer().cloned().into_iter())
            .zip_eq(buffer_offsets.iter().skip(1))
        {
            let buffer_len = buffer.len();
            self.write.write_all(buffer).await?;
            let padding = (buffer_end as usize) - current_offset - buffer_len;
            self.write.write_all(&ZEROS[0..padding]).await?;
            current_offset = buffer_end as usize;
        }

        Ok(())
    }

    async fn write_message<F: WriteFlatBuffer>(&mut self, flatbuffer: F) -> io::Result<()> {
        // We reuse the scratch buffer each time and then replace it at the end.
        // The scratch buffer may be missing if a previous write failed. We could use scopeguard
        // or similar here if it becomes a problem in practice.
        let scratch = self.scratch.take().unwrap_or_else(|| vec![]);

        let mut fbb = FlatBufferBuilder::from_vec(scratch);
        let root = flatbuffer.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(root);

        let (buffer, buffer_len) = fbb.collapse();

        let aligned_size = (buffer_len + (self.alignment - 1)) & !(self.alignment - 1);
        let padding_bytes = aligned_size - buffer_len;

        // Write the size as u32, followed by the buffer, followed by padding.
        self.write
            .write_all((aligned_size as u32).to_le_bytes())
            .await?;
        self.pos += 4;
        let buffer = self
            .write
            .write_all(buffer.slice(0, buffer_len))
            .await?
            .into_inner();
        self.pos += buffer_len as u64;
        self.write.write_all(&ZEROS[0..padding_bytes]).await?;
        self.pos += padding_bytes as u64;

        // Replace the scratch buffer
        self.scratch = Some(buffer);

        Ok(())
    }
}
