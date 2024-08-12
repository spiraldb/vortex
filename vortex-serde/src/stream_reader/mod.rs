use std::ops::Deref;
use std::sync::Arc;

use futures_util::stream::try_unfold;
use futures_util::Stream;
use vortex::stream::ArrayStream;
use vortex::Context;
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::io::VortexRead;
use crate::MessageReader;

pub struct StreamArrayReader<R: VortexRead> {
    msgs: MessageReader<R>,
    ctx: Arc<Context>,
    dtype: Option<Arc<DType>>,
}

impl<R: VortexRead> StreamArrayReader<R> {
    pub async fn try_new(read: R, ctx: Arc<Context>) -> VortexResult<Self> {
        Ok(Self {
            msgs: MessageReader::try_new(read).await?,
            ctx,
            dtype: None,
        })
    }

    pub fn with_dtype(mut self, dtype: Arc<DType>) -> Self {
        assert!(self.dtype.is_none(), "DType already set");
        self.dtype = Some(dtype);
        self
    }

    pub async fn load_dtype(mut self) -> VortexResult<Self> {
        assert!(self.dtype.is_none(), "DType already set");
        self.dtype = Some(Arc::new(self.msgs.read_dtype().await?));
        Ok(self)
    }

    /// Reads a single array from the stream.
    pub fn array_stream(&mut self) -> impl ArrayStream + '_ {
        let dtype = self.dtype.as_ref().unwrap_or_else(|| panic!("DType not set")).deref().clone();
        self.msgs.array_stream(self.ctx.clone(), dtype)
    }

    pub fn into_array_stream(self) -> impl ArrayStream {
        let dtype = self.dtype.as_ref().unwrap_or_else(|| panic!("DType not set")).deref().clone();
        self.msgs.into_array_stream(self.ctx.clone(), dtype)
    }

    /// Reads a single page from the stream.
    pub async fn next_page(&mut self) -> VortexResult<Option<Buffer>> {
        self.msgs.maybe_read_page().await
    }

    /// Reads consecutive pages from the stream until the message type changes.
    pub async fn page_stream(&mut self) -> impl Stream<Item = VortexResult<Buffer>> + '_ {
        try_unfold(self, |reader| async {
            match reader.next_page().await? {
                Some(page) => Ok(Some((page, reader))),
                None => Ok(None),
            }
        })
    }
}
