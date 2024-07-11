use std::sync::Arc;

use itertools::Itertools;
use vortex::array::struct_::{Struct, StructArray};
use vortex::validity::Validity;
use vortex::{Array, ArrayDef, Context, IntoArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::chunked_reader::ChunkedArrayReader;
use crate::io::VortexReadAt;
use crate::writer::ChunkLayout;

#[derive(Debug)]
pub struct ColumnMetadata {
    array: Array,
}

impl ColumnMetadata {
    pub fn try_new(array: Array) -> VortexResult<Self> {
        if array.encoding().id() != Struct::ID {
            vortex_bail!("Metadata table array must be a struct")
        }

        Ok(Self { array })
    }

    pub fn read<R: VortexReadAt>(
        &self,
        read: R,
        dtype: Arc<DType>,
        ctx: Arc<Context>,
    ) -> VortexResult<ChunkedArrayReader<R>> {
        ChunkedArrayReader::try_new(
            read,
            ctx,
            dtype,
            self.array
                .with_dyn(|a| a.as_struct_array().and_then(|a| a.field(0)))
                .ok_or_else(|| vortex_err!("Missing column"))?,
            self.array
                .with_dyn(|a| a.as_struct_array().and_then(|a| a.field(2)))
                .ok_or_else(|| vortex_err!("Missing column"))?,
        )
    }

    pub fn from_chunk_layout(mut layout: ChunkLayout) -> VortexResult<ColumnMetadata> {
        let len = layout.byte_offsets.len() - 1;
        let byte_counts = layout
            .byte_offsets
            .iter()
            .skip(1)
            .zip(layout.byte_offsets.iter())
            .map(|(a, b)| a - b)
            .collect_vec();
        let row_counts = layout
            .row_offsets
            .iter()
            .skip(1)
            .zip(layout.row_offsets.iter())
            .map(|(a, b)| a - b)
            .collect_vec();
        layout.byte_offsets.truncate(len);
        layout.row_offsets.truncate(len);

        let metadata_array = StructArray::try_new(
            [
                "byte_offset".into(),
                "byte_count".into(),
                "row_offset".into(),
                "row_count".into(),
            ]
            .into(),
            vec![
                layout.byte_offsets.into_array(),
                byte_counts.into_array(),
                layout.row_offsets.into_array(),
                row_counts.into_array(),
            ],
            len,
            Validity::NonNullable,
        )?;
        ColumnMetadata::try_new(metadata_array.into_array())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::executor::block_on;
    use itertools::Itertools;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::stream::ArrayStreamExt;
    use vortex::validity::Validity;
    use vortex::{ArrayDType, Context, ToArray};
    use vortex_error::VortexResult;

    use crate::file::column_metadata::ColumnMetadata;
    use crate::writer::ArrayWriter;

    #[test]
    fn read_using_metadata() -> VortexResult<()> {
        let data = PrimitiveArray::from_vec((0u64..100_000).collect_vec(), Validity::NonNullable);
        let mut buffer = Vec::new();

        let mut writer = ArrayWriter::new(buffer);
        writer = block_on(async { writer.write_array(data.to_array()).await })?;
        let meta_table =
            ColumnMetadata::from_chunk_layout(writer.array_layouts()[0].chunks.clone())?;
        buffer = writer.into_inner();

        let mut reader = meta_table.read(
            buffer,
            Arc::new(data.dtype().clone()),
            Arc::new(Context::default()),
        )?;

        let array = block_on(async { reader.array_stream().await.collect_chunked().await })?;
        assert_eq!(
            array
                .chunk(0)
                .unwrap()
                .as_primitive()
                .maybe_null_slice::<u64>(),
            data.maybe_null_slice::<u64>()
        );
        Ok(())
    }
}
