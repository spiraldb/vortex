mod take_rows;

use vortex::Array;
use vortex_error::VortexResult;

use crate::io::VortexReadAt;

/// A reader for a chunked array.
pub struct ChunkedArrayReader<R: VortexReadAt> {
    read: R,
    base_offset: usize,

    // One row per chunk + 1 row for the end of the last chunk.
    byte_offsets: Array,
    row_offsets: Array,

    // Statistics
    statistics: Array,

    nchunks: usize,
}

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub fn new(read: R, base_offset: usize, byte_offsets: Array, row_offsets: Array) -> Self {
        let nchunks = byte_offsets.len() - 1;
        assert_eq!(
            nchunks + 1,
            row_offsets.len(),
            "byte_offsets and row_offsets must have the same length"
        );
        Self {
            read,
            base_offset,
            byte_offsets,
            row_offsets,
            nchunks,
        }
    }

    pub fn nchunks(&self) -> usize {
        self.nchunks
    }
}
