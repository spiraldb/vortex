use std::future::Future;
use std::ops::DerefMut;
use std::task::Poll;

use bytes::BytesMut;
use flatbuffers::root;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};
use vortex::Array;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use super::layouts::Layout;
use super::FULL_FOOTER_SIZE;
use crate::file::file_writer::MAGIC_BYTES;
use crate::file::footer::Footer;
use crate::flatbuffers as fb;
use crate::io::VortexReadAt;

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

        // TODO: Handle cases if there's less than 8MB of total data
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
        let fb_layout = fb_footer.layout().unwrap();

        Layout::try_from(fb_layout)
    }

    pub async fn dtype(&mut self, footer: &Footer) -> VortexResult<DType> {
        let start_offset = footer.leftovers_schema_offset();
        let end_offset = footer.leftovers_footer_offset();
        let dtype_bytes = &footer.leftovers[start_offset..end_offset];

        let fb_dtype = root::<vortex_dtype::flatbuffers::DType>(dtype_bytes)?;

        DType::try_from(fb_dtype)
    }

    async fn into_stream(mut self) -> VortexResult<FileReaderStream<R>> {
        let footer = self.read_footer().await?;
        let layout = self.layout(&footer).await?;
        let dtype = self.dtype(&footer).await?;

        Ok(FileReaderStream {
            footer,
            layout,
            dtype,
            inner: self.inner,
            state: StreamingState::default(),
        })
    }
}

pub struct FileReaderStream<R> {
    footer: Footer,
    layout: Layout,
    dtype: DType,
    inner: R,
    state: StreamingState,
}

impl<R> FileReaderStream<R> {}

#[derive(Default)]
enum StreamingState {
    #[default]
    Init,
    Reading(BoxFuture<'static, VortexResult<Vec<BytesMut>>>),
    Decoding(Vec<(Layout, DType)>),
}

impl<R: VortexReadAt + Unpin> Stream for FileReaderStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    let mut layouts = Vec::default();

                    let top_level = self.layout.as_struct_mut().unwrap();

                    for c_layout in top_level.children.iter_mut() {
                        let layout = c_layout.as_chunked_mut().unwrap();

                        if layout.children.len() == 1 {
                            return Poll::Ready(None);
                        } else {
                            layouts.push(layout.children.remove(0))
                        }
                    }

                    let struct_types = self.dtype.as_struct().unwrap();

                    let r = layouts
                        .into_iter()
                        .zip(struct_types.dtypes().into_iter().cloned())
                        .collect::<Vec<_>>();

                    self.state = StreamingState::Decoding(r)
                }
                StreamingState::Decoding(layouts) => {
                    todo!("build the future")
                }
                StreamingState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok(_bytes) => todo!(),
                    Err(_e) => todo!(),
                },
            }
        }

        // if let Layout::Struct(layout) = this.layout {
        //     for c_layout in layout.children.iter_mut() {
        //         match c_layout {
        //             Layout::Chunked(l) => {
        //                 let l = l.children[self.depth].clone();
        //                 self.depth += 1;
        //             }
        //             Layout::Flat(l) => l,
        //             Layout::Struct(l) => unreachable!(),
        //         }
        //     }
        // } else {
        //     unreachable!()
        // }
        // match this.layout_ptr.as_mut() {
        //     Some(layout) => todo!(),
        //     None => todo!(),
        // }
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::struct_::StructArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::validity::Validity;
    use vortex::IntoArray;

    use super::*;
    use crate::file::file_writer::FileWriter;

    #[tokio::test]
    async fn read() {
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
        writer = writer.write_array_columns(st.into_array()).await.unwrap();
        let written = writer.finalize().await.unwrap();

        let mut reader = FileReaderBuilder::new(written).build();

        let footer = reader.read_footer().await.unwrap();

        dbg!(footer.schema_offset);
        dbg!(footer.footer_offset);
        dbg!(reader.layout(&footer).await.unwrap());
        dbg!(reader.dtype(&footer).await.unwrap());

        let mut stream = reader.into_stream().await.unwrap();

        while let Some(array) = stream.next().await {
            let _array = array.unwrap();
            println!("got an array");
        }
    }
}
