#![cfg(feature = "monoio")]

use std::future::Future;
use std::io;

use bytes::BytesMut;
use futures_util::FutureExt;
use monoio::io::{AsyncReadRent, AsyncReadRentExt};

use crate::io::VortexRead;

pub struct MonoVortexRead<R: AsyncReadRent>(R);

impl<R: AsyncReadRent> VortexRead for MonoVortexRead<R> {
    fn read_into(&mut self, buffer: BytesMut) -> impl Future<Output = io::Result<BytesMut>> {
        self.0
            .read_exact(buffer)
            .map(|(result, buffer)| match result {
                Ok(_len) => Ok(buffer),
                Err(e) => Err(e),
            })
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
    use crate::codecs::MessageReader;
    use crate::test::create_stream;

    #[monoio::test]
    async fn test_array_stream() -> VortexResult<()> {
        let buffer = create_stream();

        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
        let mut messages = MessageReader::try_new(MonoVortexRead(buffer.as_slice())).await?;

        let view_context = messages.read_view_context(&ctx).await?;
        let dtype = messages.read_dtype().await?;
        let reader = messages.next_array_reader(view_context, dtype);
        pin_mut!(reader);

        while let Some(chunk) = reader.try_next().await? {
            println!("chunk {:?}", chunk);
        }

        Ok(())
    }
}
