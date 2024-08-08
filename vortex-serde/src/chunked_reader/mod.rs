use std::io::Cursor;
use std::sync::Arc;

use vortex::compute::unary::scalar_at;
use vortex::stream::ArrayStream;
use vortex::{Array, Context};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use crate::io::VortexReadAt;
use crate::stream_reader::StreamArrayReader;

mod take_rows;

/// A reader for a chunked array.
pub struct ChunkedArrayReader<R: VortexReadAt> {
    read: R,
    context: Arc<Context>,
    dtype: Arc<DType>,

    // One row per chunk + 1 row for the end of the last chunk.
    byte_offsets: Array,
    row_offsets: Array,
}

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub fn try_new(
        read: R,
        context: Arc<Context>,
        dtype: Arc<DType>,
        byte_offsets: Array,
        row_offsets: Array,
    ) -> VortexResult<Self> {
        Self::validate(&byte_offsets, &row_offsets)?;
        Ok(Self {
            read,
            context,
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

    pub async fn array_stream(&mut self) -> impl ArrayStream + '_ {
        let mut cursor = Cursor::new(&self.read);
        cursor.set_position(u64::try_from(&scalar_at(&self.byte_offsets, 0).unwrap()).unwrap());
        StreamArrayReader::try_new(cursor, self.context.clone())
            .await
            .unwrap()
            .with_dtype(self.dtype.clone())
            .into_array_stream()
    }
}
