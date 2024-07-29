use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};
use vortex::array::struct_::StructArray;
use vortex::{Array, IntoArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError, VortexResult};

use super::layouts::Layout;
use crate::file::file_writer::MAGIC_BYTES;
use crate::file::footer::Footer;
use crate::io::VortexReadAt;
use crate::{ArrayBufferReader, ReadResult};

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
    const FOOTER_TRAILER_SIZE: usize = 20;

    pub async fn read_footer(&mut self) -> VortexResult<Footer> {
        let file_len = self.len().await as usize;
        if file_len < Self::FOOTER_TRAILER_SIZE {
            vortex_bail!(
                "Malformed vortex file, length {file_len} must be at least {}",
                Self::FOOTER_TRAILER_SIZE
            )
        }

        let read_size = Self::FOOTER_READ_SIZE.min(file_len as usize);
        let mut buf = BytesMut::with_capacity(read_size);
        unsafe { buf.set_len(read_size) }

        let read_offset = (file_len - read_size) as u64;
        buf = self.inner.read_at_into(read_offset, buf).await?;

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

    async fn len(&mut self) -> u64 {
        match self.len {
            None => {
                self.len = Some(self.inner.len().await);
                self.len.unwrap()
            }
            Some(l) => l,
        }
    }

    pub async fn into_stream(mut self) -> VortexResult<FileReaderStream<R>> {
        let footer = self.read_footer().await?;
        let layout = footer.layout()?;
        let dtype = footer.dtype()?;

        Ok(FileReaderStream {
            layout,
            dtype,
            reader: Some(self.inner),
            state: StreamingState::default(),
            context: Default::default(),
        })
    }
}

pub struct FileReaderStream<R> {
    layout: Layout,
    dtype: DType,
    reader: Option<R>,
    state: StreamingState<R>,
    context: Arc<vortex::Context>,
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
                            layouts.push(layout.children.remove(0));
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
        let layout = footer.layout().unwrap();
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
