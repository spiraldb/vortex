mod take_rows;

use std::sync::Arc;

use derive_builder::Builder;
use vortex::{Array, ViewContext};
use vortex_dtype::DType;

use crate::io::VortexReadAt;

/// A reader for a chunked array.
#[derive(Builder)]
pub struct ChunkedArrayReader<R: VortexReadAt> {
    read: R,
    view_context: Arc<ViewContext>,
    dtype: DType,

    // One row per chunk + 1 row for the end of the last chunk.
    byte_offsets: Array,
    row_offsets: Array,

    nchunks: usize,
}

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub fn nchunks(&self) -> usize {
        self.nchunks
    }
}
