use std::collections::VecDeque;
use std::mem;

use ahash::{HashMap, HashMapExt};
use futures::{Stream, TryStreamExt};
use vortex::array::{ChunkedArray, StructArray};
use vortex::stats::{ArrayStatistics, Stat};
use vortex::stream::ArrayStream;
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_scalar::Scalar;

use crate::io::VortexWrite;
use crate::layouts::read::{ChunkedLayoutSpec, ColumnLayoutSpec};
use crate::layouts::write::footer::Footer;
use crate::layouts::write::layouts::{FlatLayout, Layout, NestedLayout};
use crate::layouts::MAGIC_BYTES;
use crate::MessageWriter;

pub struct LayoutWriter<W> {
    msgs: MessageWriter<W>,

    dtype: Option<DType>,
    column_chunks: Vec<ColumnChunkAccumulator>,
}

const PRUNING_STATS: [Stat; 4] = [Stat::Min, Stat::Max, Stat::NullCount, Stat::TrueCount];

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
        let size_hint = stream.size_hint().0;
        let accumulator = match self.column_chunks.get_mut(column_idx) {
            None => {
                self.column_chunks
                    .push(ColumnChunkAccumulator::new(size_hint));
                
                assert_eq!(
                    self.column_chunks.len(),
                    column_idx + 1,
                    "write_column_chunks must be called in order by column index! got column index {} but column chunks has {} columns",
                    column_idx,
                    self.column_chunks.len()
                );

                self.column_chunks
                    .last_mut()
                    .vortex_expect("column chunks cannot be empty, just pushed")
            }
            Some(x) => x,
        };
        let mut n_rows_written = *accumulator
            .row_offsets
            .last()
            .vortex_expect("row offsets cannot be empty by construction");

        let mut byte_offsets = Vec::with_capacity(size_hint);
        byte_offsets.push(self.msgs.tell());

        while let Some(chunk) = stream.try_next().await? {
            for stat in PRUNING_STATS {
                accumulator.push_stat(stat, chunk.statistics().compute(stat));
            }

            n_rows_written += chunk.len() as u64;
            accumulator.push_row_offset(n_rows_written);

            self.msgs.write_batch(chunk).await?;
            byte_offsets.push(self.msgs.tell());
        }
        accumulator.push_batch_byte_offsets(byte_offsets);

        Ok(())
    }

    async fn write_metadata_arrays(&mut self) -> VortexResult<NestedLayout> {
        let mut column_layouts = VecDeque::with_capacity(self.column_chunks.len());
        for mut column_accumulator in mem::take(&mut self.column_chunks) {
            // we don't need the last row offset; that's just the total number of rows
            let length = column_accumulator.row_offsets.len() - 1;
            column_accumulator.row_offsets.truncate(length);

            let mut chunks: VecDeque<Layout> = column_accumulator
                .batch_byte_offsets
                .iter()
                .flat_map(|byte_offsets| {
                    byte_offsets
                        .iter()
                        .zip(byte_offsets.iter().skip(1))
                        .map(|(begin, end)| Layout::Flat(FlatLayout::new(*begin, *end)))
                })
                .collect();
            assert_eq!(chunks.len(), column_accumulator.row_offsets.len());

            let metadata_array = StructArray::try_new(
                ["row_offset".into()].into(),
                vec![column_accumulator.row_offsets.into_array()],
                length,
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
pub struct ColumnChunkAccumulator {
    pub row_offsets: Vec<u64>,
    pub batch_byte_offsets: Vec<Vec<u64>>,
    pub pruning_stats: HashMap<Stat, Vec<Scalar>>,
}

impl ColumnChunkAccumulator {
    pub fn new(size_hint: usize) -> Self {
        let mut row_offsets = Vec::with_capacity(size_hint + 1);
        row_offsets.push(0);
        Self {
            row_offsets,
            batch_byte_offsets: Vec::new(),
            pruning_stats: HashMap::with_capacity(PRUNING_STATS.len()),
        }
    }

    pub fn push_row_offset(&mut self, row_offset: u64) {
        self.row_offsets.push(row_offset);
    }

    pub fn push_batch_byte_offsets(&mut self, batch_byte_offsets: Vec<u64>) {
        self.batch_byte_offsets.push(batch_byte_offsets);
    }

    pub fn push_stat(&mut self, stat: Stat, value: Option<Scalar>) {
        self.pruning_stats.insert(stat, value);
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
