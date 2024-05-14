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
    use crate::codecs::ArrayReader;
    use crate::codecs::MessageReader;
    use crate::test::create_stream;

    #[monoio::test]
    async fn test_array_stream() -> VortexResult<()> {
        let buffer = create_stream();

        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);
        let mut messages = MessageReader::try_new(MonoVortexRead(buffer.as_slice())).await?;
        let reader = <dyn ArrayReader>::try_from_messages(&ctx, &mut messages).await?;
        pin_mut!(reader);

        while let Some(chunk) = reader.try_next().await? {
            println!("chunk {:?}", chunk);
        }

        Ok(())
    }
}
