#![cfg(feature = "tokio")]

use std::io;
use std::io::SeekFrom;

use bytes::BytesMut;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use vortex_buffer::io_buf::IoBuf;

use crate::io::{VortexRead, VortexReadAt, VortexWrite};

pub struct TokioAdapter<IO>(pub IO);

impl<R: AsyncRead + Unpin> VortexRead for TokioAdapter<R> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        self.0.read_exact(buffer.as_mut()).await?;
        Ok(buffer)
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> VortexReadAt for TokioAdapter<R> {
    async fn read_at_into(&mut self, pos: u64, mut buffer: BytesMut) -> io::Result<BytesMut> {
        self.0.seek(SeekFrom::Start(pos)).await?;
        self.0.read_exact(buffer.as_mut()).await?;
        Ok(buffer)
    }
}

impl<W: AsyncWrite + Unpin> VortexWrite for TokioAdapter<W> {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        self.0.write_all(buffer.as_slice()).await?;
        Ok(buffer)
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.0.flush().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.0.shutdown().await
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
