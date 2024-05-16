mod take_rows;

use std::sync::Arc;

use derive_builder::Builder;
use vortex::{Array, ViewContext};
use vortex_dtype::DType;

use crate::io::VortexReadAt;

/// A reader for a chunked array.
#[derive(Builder)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct ChunkedArrayReader<R: VortexReadAt> {
    read: R,
    view_context: Arc<ViewContext>,
    dtype: DType,

    // One row per chunk + 1 row for the end of the last chunk.
    byte_offsets: Array,
    row_offsets: Array,
}

impl<R: VortexReadAt> ChunkedArrayReaderBuilder<R> {
    fn validate(&self) -> Result<(), String> {
        if let (Some(byte_offsets), Some(row_offsets)) = (&self.byte_offsets, &self.row_offsets) {
            if byte_offsets.len() != row_offsets.len() {
                return Err("byte_offsets and row_offsets must have the same length".to_string());
            }
        }
        Ok(())
    }
}

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub fn nchunks(&self) -> usize {
        self.byte_offsets.len()
    }
}
