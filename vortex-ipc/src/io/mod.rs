use std::future::Future;
use std::io;

mod futures;
mod monoio;

use bytes::BytesMut;
pub use futures::*;
pub use monoio::*;

pub trait VortexRead {
    fn read_into(&mut self, buffer: BytesMut) -> impl Future<Output = io::Result<BytesMut>>;
}
