use vortex::stats::ArrayStatistics;
use vortex::Array;
use vortex_error::VortexResult;

use crate::chunked_reader::ChunkedArrayReader;
use crate::io::VortexReadAt;

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub async fn take_rows(&mut self, indices: &Array) -> VortexResult<Array> {
        // Figure out if the row indices are sorted / unique. If not, we need to sort them.
        if !indices.statistics().compute_is_strict_sorted()? {
            // If the indices are sorted, we can take the rows directly.
            return self.take_rows_strict_sorted(indices);
        }

        // Figure out which chunks are relevant to the read operation using the row_offsets array.
        // Depending on whether there are more indices than chunks, we may wish to perform this
        // join differently.

        // Coalesce the chunks we care about by some metric.

        // TODO(ngates): we could support read_into for array builders since we know the size
        //  of the result.
        // Read the relevant chunks.
        // Reshuffle the result as per the original sort order.
        unimplemented!()
    }

    /// Take rows from a chunked array given strict sorted indices.
    /// Undefined behaviour if the indices are not strict-sorted.
    async fn take_rows_strict_sorted(&mut self, indices: &Array) -> VortexResult<Array> {
        let indices_len = indices.len();

        // Figure out which chunks are relevant.
        // TODO(ngates): can this be done by search_sorted
        if self.nchunks() <= indices_len {
            // Loop over each chunk, check if it's used.
        } else {
            // Loop over each index, check which chunk it belongs to.
        }

        unimplemented!()
    }
}
