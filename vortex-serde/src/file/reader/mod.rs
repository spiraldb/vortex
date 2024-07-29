use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use filtering::RowFilter;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};
use projections::Projection;
use schema::Schema;
use vortex::array::struct_::StructArray;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::{DType, StructDType};
use vortex_error::{vortex_bail, VortexError, VortexResult};

use super::layouts::{Layout, StructLayout};
use crate::file::file_writer::MAGIC_BYTES;
use crate::file::footer::Footer;
use crate::io::VortexReadAt;
use crate::{ArrayBufferReader, ReadResult};

pub mod filtering;
pub mod projections;
pub mod schema;

pub struct VortexBatchReaderBuilder<R> {
    reader: R,
    projection: Option<Projection>,
    len: Option<u64>,
    mask: Option<Array>,
    row_filter: Option<RowFilter>,
}

impl<R: VortexReadAt> VortexBatchReaderBuilder<R> {
    const FOOTER_READ_SIZE: usize = 8 * 1024 * 1024;
    const FOOTER_TRAILER_SIZE: usize = 20;

    pub fn new(reader: R) -> Self {
        Self {
            reader,
            projection: None,
            row_filter: None,
            len: None,
            mask: None,
        }
    }

    pub fn with_length(mut self, len: u64) -> Self {
        self.len = Some(len);
        self
    }

    pub fn with_projection(mut self, projection: Projection) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_mask(mut self, array: Array) -> Self {
        // TODO(#441): Allow providing boolean masks
        assert!(
            array.dtype().is_int(),
            "Mask arrays have to be integer arrays"
        );
        self.mask = Some(array);
        self
    }

    pub fn with_row_filter(mut self, row_filter: RowFilter) -> Self {
        self.row_filter = Some(row_filter);
        self
    }

    pub async fn build(mut self) -> VortexResult<VortexBatchStream<R>> {
        let footer = self.read_footer().await?;

        // TODO(adamg): We probably want to unify everything that is going on here into a single type and implementation
        let mut layout = if let Layout::Struct(s) = footer.layout()? {
            s
        } else {
            vortex_bail!("Top level layout must be a 'StructLayout'");
        };
        let mut dtype = if let DType::Struct(s, _) = footer.dtype()? {
            s
        } else {
            vortex_bail!("Top level dtype must be a 'StructDType'");
        };

        if let Some(projection) = self.projection.as_ref() {
            layout = layout.project(projection);
            dtype = dtype.project(projection.indices())?;
        }

        Ok(VortexBatchStream {
            layout,
            dtype,
            reader: Some(self.reader),
            state: StreamingState::default(),
            context: Default::default(),
        })
    }

    async fn len(&self) -> usize {
        let len = match self.len {
            Some(l) => l,
            None => self.reader.len().await,
        };

        len as usize
    }

    pub async fn read_footer(&mut self) -> VortexResult<Footer> {
        let file_length = self.len().await;

        if file_length < Self::FOOTER_TRAILER_SIZE {
            vortex_bail!(
                "Malformed vortex file, length {} must be at least {}",
                file_length,
                Self::FOOTER_TRAILER_SIZE,
            )
        }

        let read_size = Self::FOOTER_READ_SIZE.min(file_length);
        let mut buf = BytesMut::with_capacity(read_size);
        unsafe { buf.set_len(read_size) }

        let read_offset = (file_length - read_size) as u64;
        buf = self.reader.read_at_into(read_offset, buf).await?;

        let magic_bytes_loc = read_size - MAGIC_BYTES.len();

        let magic_number = &buf[magic_bytes_loc..];
        if magic_number != MAGIC_BYTES {
            vortex_bail!("Malformed file, invalid magic bytes, got {magic_number:?}")
        }

        let footer_offset = u64::from_le_bytes(
            buf[magic_bytes_loc - 8..magic_bytes_loc]
                .try_into()
                .unwrap(),
        );
        let schema_offset = u64::from_le_bytes(
            buf[magic_bytes_loc - 16..magic_bytes_loc - 8]
                .try_into()
                .unwrap(),
        );

        Ok(Footer {
            schema_offset,
            footer_offset,
            leftovers: buf.freeze(),
            leftovers_offset: read_offset,
        })
    }
}

pub struct VortexBatchStream<R> {
    layout: StructLayout,
    dtype: StructDType,
    reader: Option<R>,
    state: StreamingState<R>,
    context: Arc<vortex::Context>,
}

impl<R> VortexBatchStream<R> {
    pub fn schema(&self) -> VortexResult<Schema> {
        Ok(Schema(self.dtype.clone()))
    }
}

type StreamStateFuture<R> = BoxFuture<'static, VortexResult<(Vec<(Arc<str>, BytesMut, DType)>, R)>>;

#[derive(Default)]
enum StreamingState<R> {
    #[default]
    Init,
    Reading(StreamStateFuture<R>),
    Decoding(Vec<ColumnInfo>),
}

struct ColumnInfo {
    layout: Layout,
    dtype: DType,
    name: Arc<str>,
}

impl ColumnInfo {
    fn new(name: Arc<str>, dtype: DType, layout: Layout) -> Self {
        Self {
            name,
            layout,
            dtype,
        }
    }
}

impl<R: VortexReadAt + Unpin + Send + 'static> Stream for VortexBatchStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    let mut layouts = Vec::default();

                    let _metadata_layouts = self
                        .layout
                        .children
                        .iter_mut()
                        .map(|c| c.as_chunked_mut().unwrap().children.pop_front().unwrap())
                        .collect::<Vec<_>>();

                    for c_layout in self.layout.children.iter_mut() {
                        let layout = c_layout.as_chunked_mut().unwrap();

                        if layout.children.is_empty() {
                            return Poll::Ready(None);
                        } else {
                            layouts.push(layout.children.pop_front().unwrap());
                        }
                    }

                    let names = self.dtype.names().iter();
                    let types = self.dtype.dtypes().iter().cloned();

                    let layouts = layouts
                        .into_iter()
                        .zip(types)
                        .zip(names)
                        .map(|((layout, dtype), name)| ColumnInfo::new(name.clone(), dtype, layout))
                        .collect();

                    self.state = StreamingState::Decoding(layouts);
                }
                StreamingState::Decoding(layouts) => {
                    let layouts = std::mem::take(layouts);
                    let reader = self.reader.take().expect("Reader should be here");

                    let f = async move {
                        let mut buffers = Vec::with_capacity(layouts.len());
                        for col_info in layouts {
                            let byte_range = col_info.layout.as_flat().unwrap().range;
                            let mut buffer = BytesMut::with_capacity(byte_range.size());
                            unsafe { buffer.set_len(byte_range.size()) };

                            let buff = reader
                                .read_at_into(byte_range.begin, buffer)
                                .await
                                .map_err(VortexError::from)
                                .map(|b| (col_info.name, b, col_info.dtype))?;
                            buffers.push(buff);
                        }

                        Ok((buffers, reader))
                    }
                    .boxed();

                    self.state = StreamingState::Reading(f)
                }
                StreamingState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((bytes, reader)) => {
                        self.reader = Some(reader);
                        let arr = bytes
                            .into_iter()
                            .map(|(name, buff, dtype)| {
                                let mut buff = buff.freeze();
                                let mut array_reader = ArrayBufferReader::new();
                                let mut read_buf = Bytes::new();
                                while let Some(ReadResult::ReadMore(u)) =
                                    array_reader.read(read_buf.clone())?
                                {
                                    read_buf = buff.split_to(u);
                                }

                                array_reader
                                    .into_array(self.context.clone(), dtype)
                                    .map(|a| (name, a))
                            })
                            .collect::<VortexResult<Vec<_>>>()?;

                        let s = StructArray::from_fields(arr.as_ref());
                        self.state = StreamingState::Init;
                        return Poll::Ready(Some(Ok(s.into_array())));
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::struct_::StructArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::validity::Validity;
    use vortex::{ArrayDType, IntoArray, IntoArrayVariant};

    use super::*;
    use crate::file::file_writer::FileWriter;

    #[tokio::test]
    async fn test_read_simple() {
        let strings = ChunkedArray::from_iter([
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        ])
        .into_array();

        let numbers = ChunkedArray::from_iter([
            PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array(),
            PrimitiveArray::from(vec![5u32, 6, 7, 8]).into_array(),
        ])
        .into_array();

        let st = StructArray::try_new(
            ["strings".into(), "numbers".into()].into(),
            vec![strings, numbers],
            8,
            Validity::NonNullable,
        )
        .unwrap();
        let buf = Vec::new();
        let mut writer = FileWriter::new(buf);
        writer = writer.write_array_columns(st.into_array()).await.unwrap();
        let written = writer.finalize().await.unwrap();

        let mut builder = VortexBatchReaderBuilder::new(written);
        let layout = builder.read_footer().await.unwrap().layout().unwrap();
        dbg!(layout);

        let mut stream = builder.build().await.unwrap();
        let mut cnt = 0;

        while let Some(array) = stream.next().await {
            let array = array.unwrap();
            assert_eq!(array.len(), 4);
            cnt += 1;
        }

        assert_eq!(cnt, 2);
    }

    #[tokio::test]
    async fn test_read_projection() {
        let strings = ChunkedArray::from_iter([
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        ])
        .into_array();

        let numbers = ChunkedArray::from_iter([
            PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array(),
            PrimitiveArray::from(vec![5u32, 6, 7, 8]).into_array(),
        ])
        .into_array();

        let st = StructArray::try_new(
            ["strings".into(), "numbers".into()].into(),
            vec![strings, numbers],
            8,
            Validity::NonNullable,
        )
        .unwrap();
        let buf = Vec::new();
        let mut writer = FileWriter::new(buf);
        writer = writer.write_array_columns(st.into_array()).await.unwrap();
        let written = writer.finalize().await.unwrap();

        let mut builder = VortexBatchReaderBuilder::new(written);
        let layout = builder.read_footer().await.unwrap().layout().unwrap();
        dbg!(layout);

        let mut stream = builder
            .with_projection(Projection::new([0]))
            .build()
            .await
            .unwrap();
        let mut cnt = 0;

        while let Some(array) = stream.next().await {
            let array = array.unwrap();
            assert_eq!(array.len(), 4);

            let array = array.into_struct().unwrap();
            let struct_dtype = array.dtype().as_struct().unwrap();
            assert_eq!(struct_dtype.dtypes().len(), 1);
            assert_eq!(struct_dtype.names()[0].as_ref(), "strings");
            cnt += 1;
        }

        assert_eq!(cnt, 2);
    }
}
