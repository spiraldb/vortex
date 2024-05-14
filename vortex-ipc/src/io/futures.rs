#![cfg(feature = "futures")]

use std::io;

use bytes::BytesMut;
use futures_util::{AsyncRead, AsyncReadExt};

use crate::io::VortexRead;

pub struct FuturesVortexRead<R: AsyncRead>(pub R);

impl<R: AsyncRead + Unpin> VortexRead for FuturesVortexRead<R> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        self.0.read_exact(buffer.as_mut()).await?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::{pin_mut, TryStreamExt};
    use vortex::encoding::EncodingRef;
    use vortex::Context;
    use vortex_alp::ALPEncoding;
    use vortex_error::VortexResult;
    use vortex_fastlanes::BitPackedEncoding;

    use super::*;
    use crate::codecs::MessageReader;
    use crate::test::create_stream;

    #[tokio::test]
    async fn test_stream() -> VortexResult<()> {
        let buffer = create_stream();

        let stream = futures_util::stream::iter(
            buffer
                .chunks(64)
                .map(|chunk| Ok(Bytes::from(chunk.to_vec()))),
        );
        let reader = stream.into_async_read();

        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
        let mut messages = MessageReader::try_new(FuturesVortexRead(reader)).await?;
        let reader = messages.array_reader_from_stream(&ctx).await?;
        pin_mut!(reader);

        while let Some(chunk) = reader.try_next().await? {
            println!("chunk {:?}", chunk);
        }

        Ok(())
    }
}
