use std::future::{ready, Future};
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

pub trait VortexReadAt {
    fn read_at_into(
        &mut self,
        pos: u64,
        buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>>;

    // TODO(ngates): the read implementation should be able to hint at its latency/throughput
    //  allowing the caller to make better decisions about how to coalesce reads.
    fn performance_hint(&self) -> usize {
        0
    }
}

impl<'a> VortexReadAt for &'a [u8] {
    fn read_at_into(
        &mut self,
        pos: u64,
        mut buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>> {
        buffer.copy_from_slice(&self[pos as usize..]);
        ready(Ok(buffer))
    }
}
