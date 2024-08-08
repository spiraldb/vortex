use std::collections::VecDeque;
use std::mem;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use vortex::array::{ChunkedArray, StructArray};
use vortex::stream::ArrayStream;
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_buffer::io_buf::IoBuf;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_flatbuffers::{footer as fb, WriteFlatBuffer};

use crate::io::VortexWrite;
use crate::layouts::reader::{ChunkedLayoutSpec, ColumnLayoutSpec};
use crate::layouts::writer::layouts::{FlatLayout, Layout, NestedLayout};
use crate::layouts::MAGIC_BYTES;
use crate::messages::IPCSchema;
use crate::writer::ChunkOffsets;
use crate::MessageWriter;

pub struct LayoutWriter<W> {
    msgs: MessageWriter<W>,

    dtype: Option<DType>,
    column_chunks: Vec<ChunkOffsets>,
}

#[derive(Debug)]
pub struct Footer {
    layout: Layout,
}

impl Footer {
    pub fn new(layout: Layout) -> Self {
        Self { layout }
    }
}

impl WriteFlatBuffer for Footer {
    type Target<'a> = fb::Footer<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let layout_offset = self.layout.write_flatbuffer(fbb);
        fb::Footer::create(
            fbb,
            &fb::FooterArgs {
                layout: Some(layout_offset),
            },
        )
    }
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
                let chunk_pos = if let Ok(chunked_array) = ChunkedArray::try_from(field.clone()) {
                    self.write_column_chunks(chunked_array.array_stream(), i)
                        .await?
                } else {
                    self.write_column_chunks(field.into_array_stream(), i)
                        .await?
                };

                self.merge_chunk_offsets(i, chunk_pos);
            }
        }

        Ok(self)
    }

    async fn write_column_chunks<S>(
        &mut self,
        mut stream: S,
        column_idx: usize,
    ) -> VortexResult<ChunkOffsets>
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
            self.msgs.write_batch(chunk).await?;
            byte_offsets.push(self.msgs.tell());
        }

        Ok(ChunkOffsets {
            byte_offsets,
            row_offsets,
        })
    }

    fn merge_chunk_offsets(&mut self, column_idx: usize, chunk_pos: ChunkOffsets) {
        if let Some(chunk) = self.column_chunks.get_mut(column_idx) {
            chunk.byte_offsets.extend(chunk_pos.byte_offsets);
            chunk.row_offsets.extend(chunk_pos.row_offsets);
        } else {
            self.column_chunks.push(chunk_pos);
        }
    }

    async fn write_metadata_arrays(&mut self) -> VortexResult<NestedLayout> {
        let DType::Struct(..) = self.dtype.as_ref().expect("Should have written values") else {
            unreachable!("Values are a structarray")
        };

        let mut column_layouts = VecDeque::with_capacity(self.column_chunks.len());

        for mut chunk in mem::take(&mut self.column_chunks) {
            let mut chunks = VecDeque::new();

            let len = chunk.byte_offsets.len() - 1;
            let byte_counts = chunk
                .byte_offsets
                .iter()
                .skip(1)
                .zip(chunk.byte_offsets.iter())
                .map(|(a, b)| a - b)
                .collect_vec();

            chunks.extend(
                chunk
                    .byte_offsets
                    .iter()
                    .zip(chunk.byte_offsets.iter().skip(1))
                    .map(|(begin, end)| Layout::Flat(FlatLayout::new(*begin, *end))),
            );
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

    async fn write_file_trailer(self, footer: Footer) -> VortexResult<W> {
        let schema_offset = self.msgs.tell();
        let mut w = self.msgs.into_inner();

        let dtype_len = Self::write_flatbuffer(
            &mut w,
            &IPCSchema(&self.dtype.expect("Needed a schema at this point")),
        )
        .await?;
        let _ = Self::write_flatbuffer(&mut w, &footer).await?;

        w.write_all(schema_offset.to_le_bytes()).await?;
        w.write_all((schema_offset + dtype_len).to_le_bytes())
            .await?;
        w.write_all(MAGIC_BYTES).await?;
        Ok(w)
    }

    // TODO(robert): Remove this once messagewriter/reader can write non length prefixed messages
    async fn write_flatbuffer<F: WriteFlatBuffer>(write: &mut W, fb: &F) -> VortexResult<u64> {
        let mut fbb = FlatBufferBuilder::new();
        let fb_offset = fb.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(fb_offset);

        let (buffer, buffer_begin) = fbb.collapse();
        let buffer_end = buffer.len();
        let sliced_buf = buffer.slice(buffer_begin, buffer_end);
        let buf_len = sliced_buf.as_slice().len() as u64;

        write.write_all(sliced_buf).await?;
        Ok(buf_len)
    }

    pub async fn finalize(mut self) -> VortexResult<W> {
        let top_level_layout = self.write_metadata_arrays().await?;
        self.write_file_trailer(Footer::new(Layout::Nested(top_level_layout)))
            .await
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use vortex::array::{PrimitiveArray, StructArray, VarBinArray};
    use vortex::validity::Validity;
    use vortex::IntoArray;

    use crate::layouts::writer::layout_writer::LayoutWriter;

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
