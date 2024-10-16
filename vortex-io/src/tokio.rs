use std::io;
use std::io::ErrorKind;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::fs::{FileExt, MetadataExt};

use bytes::BytesMut;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::{Handle, Runtime};
use vortex_buffer::io_buf::IoBuf;

use crate::read::{VortexRead, VortexReadAt};
use crate::write::VortexWrite;
use crate::Fs;

pub struct TokioFs {
    handle: Handle,
}

impl Default for TokioFs {
    fn default() -> Self {
        let handle = Handle::current();
        Self { handle }
    }
}

impl TokioFs {
    pub fn new(runtime: &Runtime) -> Self {
        Self {
            handle: runtime.handle().clone(),
        }
    }
}

impl VortexRead for File {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        self.read_exact(buffer.as_mut()).await?;
        Ok(buffer)
    }
}

impl VortexReadAt for File {
    async fn read_at_into(&self, pos: u64, mut buffer: BytesMut) -> io::Result<BytesMut> {
        // We need to perform direct pread calls directly via the file descriptor. This is not
        // something the Tokio API provides, so we need to execute raw std::fs operations, which
        // we spawn_blocking to avoid blocking the executor.
        let fd = self.as_raw_fd();

        Handle::current()
            .spawn_blocking(move || {
                // SAFETY: the fd is held open by the Tokio File (self), so it should remain open
                //  in this closure body.
                let raw_file = unsafe { std::fs::File::from_raw_fd(fd) };
                raw_file.read_exact_at(buffer.as_mut(), pos)?;

                // Avoid dropping (and closing) the file when the body exits.
                let _ = raw_file.into_raw_fd();

                Ok(buffer)
            })
            .await
            .map_err(|join_err| io::Error::new(ErrorKind::Other, join_err))?
    }

    async fn size(&self) -> u64 {
        self.metadata().await.expect("file size").size()
    }
}

impl VortexWrite for File {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        AsyncWriteExt::write_all(self, buffer.as_slice()).await?;
        Ok(buffer)
    }

    async fn flush(&mut self) -> io::Result<()> {
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}

impl Fs for TokioFs {
    type FileRead = File;
    type FileWrite = File;

    async fn open(&self, path: &str) -> io::Result<Self::FileRead> {
        File::open(path).await
    }

    async fn create(&self, path: &str) -> io::Result<Self::FileWrite> {
        File::create(path).await
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::BytesMut;
    use fusio::IoBuf;

    use crate::read::VortexReadAt;
    use crate::tokio::TokioFs;
    use crate::{Fs, VortexWrite};

    #[tokio::test]
    async fn test_tokio_fs() -> io::Result<()> {
        let fs = TokioFs::default();

        {
            let mut a = fs.create("/tmp/a").await?;
            let _ = VortexWrite::write_all(&mut a, b"abcdefgh".to_vec()).await?;
        }

        let a = fs.open("/tmp/a").await?;

        let mut buf = BytesMut::with_capacity(3);
        unsafe { buf.set_len(3) };
        let buf = a.read_at_into(5, buf).await?;
        assert_eq!(buf.as_slice(), b"fgh");

        Ok(())
    }
}
