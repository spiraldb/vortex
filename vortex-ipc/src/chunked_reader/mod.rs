use std::sync::Arc;

use vortex::{Array, ViewContext};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use crate::io::VortexReadAt;

mod take_rows;

/// A reader for a chunked array.
#[derive(Debug, Clone)]
pub struct ChunkedArrayReader<R: VortexReadAt> {
    read: R,
    view_context: Arc<ViewContext>,
    dtype: DType,

    // One row per chunk + 1 row for the end of the last chunk.
    byte_offsets: Array,
    row_offsets: Array,
}

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub fn try_new(
        read: R,
        view_context: Arc<ViewContext>,
        dtype: DType,
        byte_offsets: Array,
        row_offsets: Array,
    ) -> VortexResult<Self> {
        Self::validate(&byte_offsets, &row_offsets)?;
        Ok(Self {
            read,
            view_context,
            dtype,
            byte_offsets,
            row_offsets,
        })
    }

    pub fn nchunks(&self) -> usize {
        self.byte_offsets.len()
    }

    fn validate(byte_offsets: &Array, row_offsets: &Array) -> VortexResult<()> {
        if byte_offsets.len() != row_offsets.len() {
            vortex_bail!("byte_offsets and row_offsets must have the same length");
        }
        Ok(())
    }
}
