use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use ::flatbuffers::{root, root_unchecked};
use bytes::{Buf, BytesMut};
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};
use vortex::array::struct_::StructArray;
use vortex::{Array, ArrayView, IntoArray};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;

use super::layouts::Layout;
use super::FULL_FOOTER_SIZE;
use crate::file::file_writer::MAGIC_BYTES;
use crate::file::footer::Footer;
use crate::flatbuffers as fb;
use crate::io::VortexReadAt;
use crate::messages::IPCDType;

pub struct FileReader<R> {
    inner: R,
    len: Option<u64>,
}

pub struct FileReaderBuilder<R> {
    inner: R,
    len: Option<u64>,
}

impl<R: VortexReadAt> FileReaderBuilder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: reader,
            len: None,
        }
    }

    pub fn with_length(mut self, len: u64) -> Self {
        self.len = Some(len);
        self
    }

    pub fn build(self) -> FileReader<R> {
        FileReader {
            inner: self.inner,
            len: self.len,
        }
    }
}

impl<R: VortexReadAt> FileReader<R> {
    const FOOTER_READ_SIZE: usize = 8 * 1024 * 1024;

    pub async fn read_footer(&mut self) -> VortexResult<Footer> {
        let read_size = Self::FOOTER_READ_SIZE.min(self.len().await as usize);
        let mut buf = BytesMut::with_capacity(read_size);
        unsafe { buf.set_len(read_size) }

        let read_offset = self.len().await - read_size as u64;
        buf = self.inner.read_at_into(read_offset, buf).await?;

        let magic_bytes_loc = self.len().await as usize - MAGIC_BYTES.len();

        let magic_number = &buf[magic_bytes_loc..];
        assert_eq!(magic_number, &MAGIC_BYTES);

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

    async fn len(&mut self) -> u64 {
        match self.len {
            None => {
                self.len = Some(self.inner.len().await);
                self.len.unwrap()
            }
            Some(l) => l,
        }
    }

    pub async fn layout(&mut self, footer: &Footer) -> VortexResult<Layout> {
        let start_offset = footer.leftovers_footer_offset();
        let end_offset = footer.leftovers.len() - FULL_FOOTER_SIZE;
        let layout_bytes = &footer.leftovers[start_offset..end_offset];
        let fb_footer = root::<fb::footer::Footer>(layout_bytes)?;
        let fb_layout = fb_footer.layout().expect("Footer must contain a layout");

        Layout::try_from(fb_layout)
    }

    pub async fn dtype(&mut self, footer: &Footer) -> VortexResult<DType> {
        let start_offset = footer.leftovers_schema_offset();
        let end_offset = footer.leftovers_footer_offset();
        let dtype_bytes = &footer.leftovers[start_offset..end_offset];

        Ok(IPCDType::read_flatbuffer(&root::<fb::serde::Schema>(dtype_bytes)?)?.0)
    }

    pub async fn into_stream(mut self) -> VortexResult<FileReaderStream<R>> {
        let footer = self.read_footer().await?;
        let layout = self.layout(&footer).await?;
        let dtype = self.dtype(&footer).await?;

        Ok(FileReaderStream {
            footer,
            layout,
            dtype,
            reader: Some(self.inner),
            state: StreamingState::default(),
            context: Default::default(),
        })
    }
}

pub struct FileReaderStream<R> {
    footer: Footer,
    layout: Layout,
    dtype: DType,
    reader: Option<R>,
    state: StreamingState<R>,
    context: Arc<vortex::Context>,
}

impl<R> FileReaderStream<R> {}

#[derive(Default)]
enum StreamingState<R> {
    #[default]
    Init,
    Reading(BoxFuture<'static, VortexResult<(Vec<(Arc<str>, BytesMut, DType)>, R)>>),
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

impl<R: VortexReadAt + Unpin + Send + 'static> Stream for FileReaderStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    let mut layouts = Vec::default();
                    let struct_types = self.dtype.as_struct().unwrap().clone();
                    let top_level = self.layout.as_struct_mut().unwrap();

                    for c_layout in top_level.children.iter_mut() {
                        let layout = c_layout.as_chunked_mut().unwrap();

                        if layout.children.len() == 1 {
                            return Poll::Ready(None);
                        } else {
                            layouts.push(layout.children.remove(0))
                        }
                    }

                    let names = struct_types.names().iter();
                    let types = struct_types.dtypes().iter().cloned();

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

                        let buffers = buffers.into_iter().collect::<Vec<_>>();

                        VortexResult::Ok((buffers, reader))
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

                                let len_header = buff.split_to(size_of::<u32>());
                                let len =
                                    u32::from_le_bytes(len_header[..].try_into().unwrap()) as usize;

                                let fb_bytes = buff.split_to(len);
                                let buffers_total_len = buff.len();

                                let batch = root::<fb::serde::Message>(&fb_bytes)?
                                    .header_as_batch()
                                    .unwrap();

                                let batch_len = batch.length() as usize;

                                let ipc_buffers = batch.buffers().unwrap_or_default();

                                let buffers = ipc_buffers
                                    .iter()
                                    .zip(
                                        ipc_buffers
                                            .iter()
                                            .map(|b| b.offset())
                                            .skip(1)
                                            .chain([buffers_total_len as u64]),
                                    )
                                    .map(|(buffer, next_offset)| {
                                        let buffer_len =
                                            next_offset - buffer.offset() - buffer.padding() as u64;

                                        // Grab the buffer
                                        let data_buffer = buff.split_to(buffer_len as usize);
                                        // Strip off any padding from the previous buffer
                                        buff.advance(buffer.padding() as usize);

                                        Buffer::from(data_buffer)
                                    })
                                    .collect::<Vec<_>>();

                                let array_view = ArrayView::try_new(
                                    self.context.clone(),
                                    dtype,
                                    batch_len,
                                    Buffer::Bytes(fb_bytes),
                                    |flatbuffer| {
                                        root::<crate::flatbuffers::serde::Message>(flatbuffer)
                                            .header_as_batch()
                                            .expect("Header is not a batch")
                                            .array()
                                            .ok_or_else(|| vortex_err!("Chunk missing Array"))
                                    },
                                    buffers,
                                )?
                                .into_array();

                                Ok((name, array_view))
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
    use vortex::IntoArray;

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

        let mut reader = FileReaderBuilder::new(written).build();

        let footer = reader.read_footer().await.unwrap();
        let layout = reader.layout(&footer).await.unwrap();
        dbg!(layout);

        let mut stream = reader.into_stream().await.unwrap();

        let mut cnt = 0;

        while let Some(array) = stream.next().await {
            let array = array.unwrap();
            assert_eq!(array.len(), 4);
            cnt += 1;
        }

        assert_eq!(cnt, 2);
    }
}
