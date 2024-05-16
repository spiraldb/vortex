use std::collections::BTreeSet;

use bytes::BytesMut;
use futures_util::pin_mut;
use itertools::Itertools;
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::take::take;
use vortex::stats::ArrayStatistics;
use vortex::{Array, IntoArray};
use vortex_error::VortexResult;

use crate::chunked_reader::ChunkedArrayReader;
use crate::io::{FuturesAdapter, VortexReadAt};
use crate::MessageReader;

impl<R: VortexReadAt> ChunkedArrayReader<R> {
    pub async fn take_rows(&mut self, indices: &Array) -> VortexResult<Array> {
        // Figure out if the row indices are sorted / unique. If not, we need to sort them.
        if indices
            .statistics()
            .compute_is_strict_sorted()
            .unwrap_or(false)
        {
            // With strict-sorted indices, we can take the rows directly.
            return self.take_rows_strict_sorted(indices).await;
        }

        //         // Figure out which chunks are relevant to the read operation using the row_offsets array.
        //         // Depending on whether there are more indices than chunks, we may wish to perform this
        //         // join differently.
        //
        //         // Coalesce the chunks we care about by some metric.
        //
        //         // TODO(ngates): we could support read_into for array builders since we know the size
        //         //  of the result.
        //         // Read the relevant chunks.
        // Reshuffle the result as per the original sort order.
        unimplemented!()
    }

    /// Take rows from a chunked array given strict sorted indices.
    ///
    /// The strategy for doing this depends on the quantity and distribution of the indices...
    ///
    /// For now, we will find the relevant chunks, coalesce them, and read.
    async fn take_rows_strict_sorted(&mut self, indices: &Array) -> VortexResult<Array> {
        let indices_len = indices.len();

        // Figure out which chunks are relevant.
        let chunk_idxs = find_chunks(&self.row_offsets, indices)?;

        // Coalesce the chunks that we're going to read from.
        let coalesced_chunks = self.coalesce_chunks(&chunk_idxs);

        // Grab the row and byte offsets for each chunk range.
        let start_chunks = PrimitiveArray::from(
            coalesced_chunks
                .iter()
                .map(|chunk_range| chunk_range.start)
                .collect_vec(),
        )
        .into_array();
        let start_rows = take(&self.row_offsets, &start_chunks)?.flatten_primitive()?;
        let start_bytes = take(&self.byte_offsets, &start_chunks)?.flatten_primitive()?;

        let stop_chunks = PrimitiveArray::from(
            coalesced_chunks
                .iter()
                .map(|chunk_range| chunk_range.stop)
                .collect_vec(),
        )
        .into_array();
        let stop_rows = take(&self.row_offsets, &stop_chunks)?.flatten_primitive()?;
        let stop_bytes = take(&self.byte_offsets, &stop_chunks)?.flatten_primitive()?;

        // For each chunk-range, read the data as an ArrayStream and call take on it.
        for (range_idx, chunk_range) in coalesced_chunks.into_iter().enumerate() {
            let (start_byte, stop_byte) = (
                start_bytes.get_as_cast::<u64>(range_idx),
                stop_bytes.get_as_cast::<u64>(range_idx),
            );
            let range_byte_len = (stop_byte - start_byte) as usize;
            let (start_row, stop_row) = (
                start_rows.get_as_cast::<u64>(range_idx),
                stop_rows.get_as_cast::<u64>(range_idx),
            );
            let range_row_len = (stop_row - start_row) as usize;

            let mut buffer = BytesMut::with_capacity(range_byte_len);
            unsafe { buffer.set_len(range_byte_len) }
            let buffer = self.read.read_at_into(start_byte, buffer).await?;

            let mut msgs = MessageReader::try_new(FuturesAdapter(buffer.as_ref())).await?;
            let stream = msgs.array_stream(self.view_context.clone(), self.dtype.clone());
            pin_mut!(stream);

            todo!()
        }

        unimplemented!()
    }

    /// Coalesce reads for the given chunks.
    ///
    /// This depends on a few factors:
    /// * The number of bytes between adjacent selected chunks.
    /// * The latency of the underlying storage.
    /// * The throughput of the underlying storage.
    fn coalesce_chunks(&self, chunk_idxs: &BTreeSet<u32>) -> Vec<ChunkRange> {
        let _hint = self.read.performance_hint();

        unimplemented!()
    }
}

struct ChunkRange {
    start: u32,
    stop: u32, // Exclusive
}

impl ChunkRange {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u32 {
        self.stop - self.start
    }
}

fn find_chunks(row_offsets: &Array, indices: &Array) -> VortexResult<BTreeSet<u32>> {
    todo!()
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;
    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::{IntoArray, ViewContext};
    use vortex_dtype::PType;

    use crate::chunked_reader::ChunkedArrayReaderBuilder;
    use crate::stream_writer::ArrayWriter;

    async fn chunked_array() -> ArrayWriter<Vec<u8>> {
        let c = ChunkedArray::try_new(
            vec![PrimitiveArray::from((0i32..1000).collect_vec()).into_array(); 10],
            PType::I32.into(),
        )
        .unwrap()
        .into_array();

        ArrayWriter::new(vec![], ViewContext::default())
            .write_context()
            .await
            .unwrap()
            .write_array(c)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_take_rows() {
        let writer = chunked_array().await;

        let row_offsets = PrimitiveArray::from(
            writer
                .array_layouts()
                .first()
                .unwrap()
                .chunks
                .row_offsets
                .clone(),
        );
        let byte_offsets = PrimitiveArray::from(
            writer
                .array_layouts()
                .first()
                .unwrap()
                .chunks
                .byte_offsets
                .clone(),
        );

        let mut reader = ChunkedArrayReaderBuilder::default()
            .read(writer.into_write())
            .view_context(Arc::new(ViewContext::default()))
            .dtype(PType::I32.into())
            .row_offsets(row_offsets.into_array())
            .byte_offsets(byte_offsets.into_array())
            .build()
            .unwrap();

        let result = reader
            .take_rows(&PrimitiveArray::from(vec![0, 10, 100_000]).into_array())
            .await
            .unwrap();
        unimplemented!()
    }
}
