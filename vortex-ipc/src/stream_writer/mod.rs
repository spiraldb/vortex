mod reader;

use std::future::Future;

use futures_util::TryStreamExt;
use vortex::{Array, ArrayDType, IntoArrayData, ViewContext};
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
        self.with_range(|| async { self.msgs.write_view_context(&self.view_ctx).await })
    }

    pub async fn write_array(&mut self, array: Array) -> VortexResult<ByteRange> {
        self.with_range(|| async {
            self.msgs.write_dtype(array.dtype()).await?;
            self.msgs
                .write_chunk(&self.view_ctx, array.into_array_data())
                .await
        })
    }

    pub async fn write_array_stream<S: ArrayStream + Unpin>(
        &mut self,
        mut array_stream: S,
    ) -> VortexResult<ByteRange> {
        self.with_range(|| async {
            self.msgs.write_dtype(array_stream.dtype()).await?;
            while let Some(array) = array_stream.try_next().await? {
                self.msgs
                    .write_chunk(&self.view_ctx, array.into_array_data())
                    .await?;
            }
        })
    }

    async fn with_range<F, Fut>(&mut self, f: F) -> VortexResult<ByteRange>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = VortexResult<()>>,
    {
        let begin = self.msgs.tell();
        f().await?;
        let end = self.msgs.tell() - begin;
        Ok(ByteRange { begin, end })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ByteRange {
    pub begin: usize,
    pub end: usize,
}
