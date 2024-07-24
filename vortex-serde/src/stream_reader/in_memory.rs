use std::sync::Arc;

use vortex::Context;
use vortex_dtype::DType;

use crate::io::VortexSyncRead;

pub struct InMemoryArrayReader<R: VortexSyncRead> {
    read: R,
    ctx: Arc<Context>,
    dtype: Option<Arc<DType>>,
}

impl<R: VortexSyncRead> InMemoryArrayReader<R> {
    pub fn new(read: R, ctx: Arc<Context>) -> Self {
        Self {
            read,
            ctx,
            dtype: None,
        }
    }

    pub fn with_dtype(mut self, dtype: Arc<DType>) -> Self {
        assert!(self.dtype.is_none(), "DType already set");
        self.dtype = Some(dtype);
        self
    }
}
