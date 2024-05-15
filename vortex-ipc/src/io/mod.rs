use std::future::Future;
use std::io;

mod futures;
mod monoio;
mod tokio;

use bytes::BytesMut;
#[cfg(feature = "futures")]
pub use futures::*;
#[cfg(feature = "monoio")]
pub use monoio::*;
#[cfg(feature = "tokio")]
pub use tokio::*;

pub trait VortexRead {
    fn read_into(&mut self, buffer: BytesMut) -> impl Future<Output = io::Result<BytesMut>>;
}
