use std::collections::VecDeque;
use std::mem;

use futures::{Stream, TryStreamExt};
use vortex::array::{ChunkedArray, StructArray};
use vortex::stream::ArrayStream;
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::io::VortexWrite;
use crate::layouts::read::{ChunkedLayoutSpec, ColumnLayoutSpec};
use crate::layouts::write::footer::Footer;
use crate::layouts::write::layouts::{FlatLayout, Layout, NestedLayout};
use crate::layouts::MAGIC_BYTES;
use crate::MessageWriter;

pub struct LayoutWriter<W> {
    msgs: MessageWriter<W>,

    dtype: Option<DType>,
    column_chunks: Vec<BatchOffsets>,
}

impl<W: VortexWrite> LayoutWriter<W> {
    pub fn new(write: W) -> Self {
        LayoutWriter {
            msgs: MessageWriter::new(write),
            dtype: None,
            column_chunks: Vec::new(),
        }
    }

    pub async fn write_array_columns(self, array: Array) -> VortexResult<Self> {
        if let Ok(chunked) = ChunkedArray::try_from(&array) {
            self.write_array_columns_stream(chunked.array_stream())
                .await
        } else {
            self.write_array_columns_stream(array.into_array_stream())
                .await
        }
    }

    pub async fn write_array_columns_stream<S: ArrayStream + Unpin>(
        mut self,
        mut array_stream: S,
    ) -> VortexResult<Self> {
        match self.dtype {
            None => self.dtype = Some(array_stream.dtype().clone()),
            Some(ref sd) => {
                if sd != array_stream.dtype() {
                    vortex_bail!(
                        "Expected all arrays in the stream to have the same dtype {}, found {}",
                        sd,
                        array_stream.dtype()
                    )
                }
            }
        }

        while let Some(columns) = array_stream.try_next().await? {
            let st = StructArray::try_from(&columns)?;
            for (i, field) in st.children().enumerate() {
                if let Ok(chunked_array) = ChunkedArray::try_from(field.clone()) {
                    self.write_column_chunks(chunked_array.array_stream(), i)
                        .await?
                } else {
                    self.write_column_chunks(field.into_array_stream(), i)
                        .await?
                }
            }
        }

        Ok(self)
    }

    async fn write_column_chunks<S>(&mut self, mut stream: S, column_idx: usize) -> VortexResult<()>
    where
        S: Stream<Item = VortexResult<Array>> + Unpin,
    {
        let column_row_offset = self
            .column_chunks
            .get(column_idx)
            .and_then(|c| c.row_offsets.back())
            .copied()
            .unwrap_or(0u64);
        let mut byte_offsets = vec![self.msgs.tell()];
        let mut row_offsets = VecDeque::new();
        row_offsets.push_front(column_row_offset);

        while let Some(chunk) = stream.try_next().await? {
            row_offsets.push_back(
                row_offsets
                    .back()
                    .map(|off| off + chunk.len() as u64)
                    .ok_or_else(|| vortex_err!("Row offsets should be initialized with a value"))?,
            );
            self.msgs.write_batch(chunk).await?;
            byte_offsets.push(self.msgs.tell());
        }

        if let Some(batches) = self.column_chunks.get_mut(column_idx) {
            // Remove last entry from the list as it would be the same as first entry of next chunk
            row_offsets.pop_front();

            batches.batch_byte_offsets.push(byte_offsets);
            batches.row_offsets.extend(row_offsets);
        } else {
            self.column_chunks
                .push(BatchOffsets::new(vec![byte_offsets], row_offsets));
        }

        Ok(())
    }

    async fn write_metadata_arrays(&mut self) -> VortexResult<NestedLayout> {
        let mut column_layouts = VecDeque::with_capacity(self.column_chunks.len());
        for mut chunk in mem::take(&mut self.column_chunks) {
            let mut chunks: VecDeque<Layout> = chunk
                .batch_byte_offsets
                .iter()
                .flat_map(|byte_offsets| {
                    byte_offsets
                        .iter()
                        .zip(byte_offsets.iter().skip(1))
                        .map(|(begin, end)| Layout::Flat(FlatLayout::new(*begin, *end)))
                })
                .collect();
            let len = chunk.row_offsets.len() - 1;
            chunk.row_offsets.truncate(len);

            let offset_vec: Vec<u64> = chunk.row_offsets.into();
            let metadata_array = StructArray::try_new(
                ["row_offset".into()].into(),
                vec![offset_vec.into_array()],
                len,
                Validity::NonNullable,
            )?;

            let metadata_table_begin = self.msgs.tell();
            self.msgs.write_dtype(metadata_array.dtype()).await?;
            self.msgs.write_batch(metadata_array.into_array()).await?;
            chunks.push_front(Layout::Flat(FlatLayout::new(
                metadata_table_begin,
                self.msgs.tell(),
            )));
            column_layouts.push_back(Layout::Nested(NestedLayout::new(
                chunks,
                ChunkedLayoutSpec::ID,
            )));
        }

        Ok(NestedLayout::new(column_layouts, ColumnLayoutSpec::ID))
    }

    async fn write_footer(&mut self, footer: Footer) -> VortexResult<(u64, u64)> {
        let dtype_offset = self.msgs.tell();
        self.msgs
            .write_dtype(
                &self
                    .dtype
                    .take()
                    .ok_or_else(|| vortex_err!("Schema should be written by now"))?,
            )
            .await?;
        let footer_offset = self.msgs.tell();
        self.msgs.write_message(footer).await?;
        Ok((dtype_offset, footer_offset))
    }

    pub async fn finalize(mut self) -> VortexResult<W> {
        let top_level_layout = self.write_metadata_arrays().await?;
        let (dtype_offset, footer_offset) = self
            .write_footer(Footer::new(Layout::Nested(top_level_layout)))
            .await?;
        let mut w = self.msgs.into_inner();

        w.write_all(dtype_offset.to_le_bytes()).await?;
        w.write_all(footer_offset.to_le_bytes()).await?;
        w.write_all(MAGIC_BYTES).await?;
        Ok(w)
    }
}

#[derive(Clone, Debug)]
pub struct BatchOffsets {
    pub batch_byte_offsets: Vec<Vec<u64>>,
    pub row_offsets: VecDeque<u64>,
}

impl BatchOffsets {
    pub fn new(batch_byte_offsets: Vec<Vec<u64>>, row_offsets: VecDeque<u64>) -> Self {
        Self {
            batch_byte_offsets,
            row_offsets,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use vortex::array::{PrimitiveArray, StructArray, VarBinArray};
    use vortex::validity::Validity;
    use vortex::IntoArray;

    use crate::layouts::LayoutWriter;

    #[test]
    fn write_columns() {
        let strings = VarBinArray::from(vec!["ab", "foo", "bar", "baz"]);
        let numbers = PrimitiveArray::from(vec![1u32, 2, 3, 4]);
        let st = StructArray::try_new(
            ["strings".into(), "numbers".into()].into(),
            vec![strings.into_array(), numbers.into_array()],
            4,
            Validity::NonNullable,
        )
        .unwrap();
        let buf = Vec::new();
        let mut writer = LayoutWriter::new(buf);
        writer = block_on(async { writer.write_array_columns(st.into_array()).await }).unwrap();
        let written = block_on(async { writer.finalize().await }).unwrap();
        assert!(!written.is_empty());
    }
}
