use futures_util::{Stream, TryStreamExt};
use vortex::{Array, IntoArrayData, ViewContext};
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::array_stream::ArrayStream;
use crate::io::VortexWrite;
use crate::MessageWriter;

pub struct ArrayWriter<W: VortexWrite> {
    msgs: MessageWriter<W>,
    view_ctx: ViewContext,
}

impl<W: VortexWrite> ArrayWriter<W> {
    pub fn new(write: W, view_ctx: ViewContext) -> Self {
        Self {
            msgs: MessageWriter::new(write),
            view_ctx,
        }
    }

    pub fn into_write(self) -> W {
        self.msgs.into_write()
    }

    pub async fn write_context(&mut self) -> VortexResult<ByteRange> {
        let begin = self.msgs.tell();
        self.msgs.write_view_context(&self.view_ctx).await?;
        let end = self.msgs.tell();
        Ok(ByteRange { begin, end })
    }

    pub async fn write_dtype(&mut self, dtype: &DType) -> VortexResult<ByteRange> {
        let begin = self.msgs.tell();
        self.msgs.write_dtype(dtype).await?;
        let end = self.msgs.tell();
        Ok(ByteRange { begin, end })
    }

    pub async fn write_array_chunks<S>(&mut self, mut stream: S) -> VortexResult<ChunkPositions>
    where
        S: Stream<Item = VortexResult<Array>> + Unpin,
    {
        let mut byte_offsets = vec![0];
        let mut row_offsets = vec![0];
        let mut row_offset = 0;

        while let Some(chunk) = stream.try_next().await? {
            row_offset += chunk.len() as u64;
            row_offsets.push(row_offset);
            self.msgs
                .write_chunk(&self.view_ctx, chunk.into_array_data())
                .await?;
            byte_offsets.push(self.msgs.tell());
        }

        Ok(ChunkPositions {
            byte_offsets,
            row_offsets,
        })
    }

    pub async fn write_array_stream<S: ArrayStream + Unpin>(
        &mut self,
        mut array_stream: S,
    ) -> VortexResult<ArrayPosition> {
        let dtype_pos = self.write_dtype(array_stream.dtype()).await?;
        let chunk_pos = self.write_array_chunks(&mut array_stream).await?;
        Ok(ArrayPosition {
            dtype: dtype_pos,
            chunks: chunk_pos,
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ByteRange {
    pub begin: u64,
    pub end: u64,
}

#[derive(Clone, Debug)]
pub struct ArrayPosition {
    dtype: ByteRange,
    chunks: ChunkPositions,
}

#[derive(Clone, Debug)]
pub struct ChunkPositions {
    byte_offsets: Vec<u64>,
    row_offsets: Vec<u64>,
}
