use std::mem;

use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use vortex::array::chunked::ChunkedArray;
use vortex::array::struct_::StructArray;
use vortex::array::varbin::builder::VarBinBuilder;
use vortex::stream::ArrayStream;
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, VortexResult};

use crate::io::VortexWrite;
use crate::writer::ChunkLayout;
use crate::MessageWriter;

pub const MAGIC_BYTES: [u8; 4] = *b"SP1R";

pub struct FileWriter<W> {
    msgs: MessageWriter<W>,

    dtype: Option<DType>,
    column_chunks: Vec<ChunkLayout>,
}

impl<W: VortexWrite> FileWriter<W> {
    pub fn new(write: W) -> Self {
        FileWriter {
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
                let chunk_pos = self
                    .write_column_chunks(field.into_array_stream(), i)
                    .await?;

                self.merge_chunk_offsets(i, chunk_pos);
            }
        }

        Ok(self)
    }

    async fn write_column_chunks<S>(
        &mut self,
        mut stream: S,
        column_idx: usize,
    ) -> VortexResult<ChunkLayout>
    where
        S: Stream<Item = VortexResult<Array>> + Unpin,
    {
        let column_row_offset = self
            .column_chunks
            .get(column_idx)
            .and_then(|c| c.row_offsets.last())
            .copied()
            .unwrap_or(0u64);
        let mut byte_offsets = vec![self.msgs.tell()];
        let mut row_offsets = vec![column_row_offset];

        while let Some(chunk) = stream.try_next().await? {
            row_offsets.push(
                row_offsets
                    .last()
                    .map(|off| off + chunk.len() as u64)
                    .expect("Row offsets should be initialized with a value"),
            );
            self.msgs.write_chunk(chunk).await?;
            byte_offsets.push(self.msgs.tell());
        }

        Ok(ChunkLayout {
            byte_offsets,
            row_offsets,
        })
    }

    fn merge_chunk_offsets(&mut self, column_idx: usize, chunk_pos: ChunkLayout) {
        if let Some(chunk) = self.column_chunks.get_mut(column_idx) {
            chunk.byte_offsets.extend(chunk_pos.byte_offsets);
            chunk.row_offsets.extend(chunk_pos.row_offsets);
        } else {
            self.column_chunks.push(chunk_pos);
        }
    }

    async fn write_metadata_arrays(&mut self) -> VortexResult<Array> {
        let DType::Struct(s, _) = self.dtype.as_ref().expect("Should have written values") else {
            unreachable!("Values are a structarray")
        };

        let mut column_names = VarBinBuilder::<u32>::with_capacity(s.names().len());
        let mut metadata_offsets = Vec::new();

        for (name, mut chunk) in s.names().iter().zip(mem::take(&mut self.column_chunks)) {
            let len = chunk.byte_offsets.len() - 1;
            let byte_counts = chunk
                .byte_offsets
                .iter()
                .skip(1)
                .zip(chunk.byte_offsets.iter())
                .map(|(a, b)| a - b)
                .collect_vec();
            let row_counts = chunk
                .row_offsets
                .iter()
                .skip(1)
                .zip(chunk.row_offsets.iter())
                .map(|(a, b)| a - b)
                .collect_vec();
            chunk.byte_offsets.truncate(len);
            chunk.row_offsets.truncate(len);

            let metadata_array = StructArray::try_new(
                [
                    "byte_offset".into(),
                    "byte_count".into(),
                    "row_offset".into(),
                    "row_count".into(),
                ]
                .into(),
                vec![
                    chunk.byte_offsets.into_array(),
                    byte_counts.into_array(),
                    chunk.row_offsets.into_array(),
                    row_counts.into_array(),
                ],
                len,
                Validity::NonNullable,
            )?;

            column_names.push_value(name.as_bytes());
            metadata_offsets.push(self.msgs.tell());
            self.msgs.write_dtype(metadata_array.dtype()).await?;
            self.msgs.write_chunk(metadata_array.into_array()).await?;
        }

        let meta_array = StructArray::try_new(
            ["names".into(), "metadata_offsets".into()].into(),
            vec![
                column_names
                    .finish(DType::Utf8(Nullability::NonNullable))
                    .into_array(),
                metadata_offsets.into_array(),
            ],
            s.names().len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();

        Ok(meta_array)
    }

    async fn write_metadata_offsets(&mut self, metadata_offsets: Array) -> VortexResult<u64> {
        let offset = self.msgs.tell();

        self.msgs.write_dtype(metadata_offsets.dtype()).await?;
        self.msgs.write_chunk(metadata_offsets).await?;
        Ok(offset)
    }

    async fn write_file_trailer(self, metadata_offsets_offset: u64) -> VortexResult<W> {
        let mut w = self.msgs.into_inner();
        w.write_all(metadata_offsets_offset.to_le_bytes()).await?;
        w.write_all(MAGIC_BYTES).await?;
        Ok(w)
    }

    pub async fn finalize(mut self) -> VortexResult<W> {
        let metadata_offsets = self.write_metadata_arrays().await?;
        let metadata_offsets = self.write_metadata_offsets(metadata_offsets).await?;
        self.write_file_trailer(metadata_offsets).await
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::struct_::StructArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::validity::Validity;
    use vortex::IntoArray;

    use crate::file::file_writer::FileWriter;

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
        let mut writer = FileWriter::new(buf);
        writer = block_on(async { writer.write_array_columns(st.into_array()).await }).unwrap();
        let written = block_on(async { writer.finalize().await }).unwrap();
        assert!(!written.is_empty());
    }
}
