#![cfg(feature = "monoio")]

use std::future::Future;
use std::io;

use bytes::BytesMut;
use futures_util::FutureExt;
use monoio::buf::IoBufMut;
use monoio::io::{AsyncReadRent, AsyncReadRentExt, AsyncWriteRent, AsyncWriteRentExt};
use vortex_buffer::io_buf::IoBuf;

use crate::io::{VortexRead, VortexWrite};

pub struct MonoAdapter<IO>(IO);

impl<R: AsyncReadRent> VortexRead for MonoAdapter<R> {
    fn read_into(&mut self, buffer: BytesMut) -> impl Future<Output = io::Result<BytesMut>> {
        let len = buffer.len();
        self.0
            .read_exact(buffer.slice_mut(0..len))
            .map(|(result, buffer)| match result {
                Ok(_len) => Ok(buffer.into_inner()),
                Err(e) => Err(e),
            })
    }
}

impl<W: AsyncWriteRent> VortexWrite for MonoAdapter<W> {
    fn write_all<B: IoBuf>(&mut self, buffer: B) -> impl Future<Output = io::Result<B>> {
        self.0
            .write_all(MonoAdapter(buffer))
            .map(|(result, buffer)| match result {
                Ok(_len) => Ok(buffer.0),
                Err(e) => Err(e),
            })
    }

    fn flush(&mut self) -> impl Future<Output = io::Result<()>> {
        self.0.flush()
    }

    fn shutdown(&mut self) -> impl Future<Output = io::Result<()>> {
        self.0.shutdown()
    }
}

unsafe impl<B: IoBuf> monoio::buf::IoBuf for MonoAdapter<B> {
    fn read_ptr(&self) -> *const u8 {
        IoBuf::read_ptr(&self.0)
    }

    fn bytes_init(&self) -> usize {
        IoBuf::bytes_init(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use futures_util::pin_mut;
    use futures_util::TryStreamExt;
    use vortex::encoding::EncodingRef;
    use vortex::Context;
    use vortex_alp::ALPEncoding;
    use vortex_error::VortexResult;
    use vortex_fastlanes::BitPackedEncoding;

    use super::*;
    use crate::test::create_stream;
    use crate::MessageReader;

    #[monoio::test]
    async fn test_array_stream() -> VortexResult<()> {
        let buffer = create_stream();

        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
        let mut messages = MessageReader::try_new(MonoAdapter(buffer.as_slice())).await?;
        let reader = messages.array_stream_from_messages(&ctx).await?;
        pin_mut!(reader);

        while let Some(chunk) = reader.try_next().await? {
            println!("chunk {:?}", chunk);
        }

        Ok(())
    }
}
