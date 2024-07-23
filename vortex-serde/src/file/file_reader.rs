use bytes::BytesMut;
use flatbuffers::root;
use futures::Stream;
use vortex::Array;
use vortex_error::VortexResult;

use super::layouts::Layout;
use crate::file::file_writer::MAGIC_BYTES;
use crate::file::footer::Footer;
use crate::flatbuffers::footer as fb;
use crate::io::VortexReadAt;

pub struct FileReader<R> {
    read: R,
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
            read: self.inner,
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
        buf = self.read.read_at_into(read_offset, buf).await?;

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
                self.len = Some(self.read.len().await);
                self.len.unwrap()
            }
            Some(l) => l,
        }
    }

    async fn layout(&mut self, footer: Footer) -> VortexResult<Layout> {
        let leftover_footer_offset = dbg!(footer.leftovers_footer_offset());
        let end = dbg!(footer.leftovers.len() - 20);
        let footer_bytes = &footer.leftovers[leftover_footer_offset..end];
        let fb_footer = root::<fb::Footer>(footer_bytes)?;
        let fb_layout = fb_footer.layout().unwrap();

        Layout::try_from(fb_layout)
    }
}

impl<R: VortexReadAt> Stream for FileReader<R> {
    type Item = VortexResult<Array>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
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
        dbg!(reader.layout(footer).await.unwrap());

        while let Some(array) = reader.next().await {
            let _array = array.unwrap();
            println!("got an array");
        }
    }
}
