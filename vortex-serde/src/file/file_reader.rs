use bytes::BytesMut;
use vortex_error::VortexResult;

use crate::file::file_metadata::FileMetadata;
use crate::io::VortexReadAt;

pub struct FileReader<R> {
    read: R,
    len: Option<u64>,
}

impl<R: VortexReadAt> FileReader<R> {
    const FOOTER_READ_SIZE: usize = 8 * 1024 * 1024;

    pub fn new(read: R) -> Self {
        Self { read, len: None }
    }

    pub fn with_length(mut self, len: u64) -> Self {
        self.len = Some(len);
        self
    }

    pub async fn read_metadata(&mut self, _columns: &[&str]) -> VortexResult<FileMetadata> {
        let mut buf = BytesMut::with_capacity(Self::FOOTER_READ_SIZE);
        unsafe { buf.set_len(Self::FOOTER_READ_SIZE) }
        let read_offset = self.len().await - Self::FOOTER_READ_SIZE as u64;
        buf = self.read.read_at_into(read_offset, buf).await?;

        // Ok(FileMetadata {})
        todo!()
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn read() {}
}
