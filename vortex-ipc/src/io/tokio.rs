#![cfg(feature = "tokio")]
use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::io::VortexRead;

pub struct TokioVortexRead<R: AsyncRead>(pub R);

impl<R: AsyncRead + Unpin> VortexRead for TokioVortexRead<R> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        self.0.read_exact(buffer.as_mut()).await?;
        Ok(buffer)
    }
}
