use std::future::Future;
use std::io;

mod futures;
mod monoio;

use bytes::BytesMut;
#[cfg(feature = "futures")]
pub use futures::*;
#[cfg(feature = "monoio")]
pub use monoio::*;

pub trait VortexRead {
    fn read_into(&mut self, buffer: BytesMut) -> impl Future<Output = io::Result<BytesMut>>;
}

pub trait VortexReadAt {
    fn read_at_into(
        &mut self,
        pos: u64,
        buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>>;
}
