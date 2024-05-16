use std::future::{ready, Future};
use std::io;
use std::io::{Cursor, Read};
use std::task::ready;

use bytes::BytesMut;
use monoio::buf::IoBufMut;
use tokio::io::AsyncReadExt;

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

impl VortexRead for Cursor<Vec<u8>> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        Read::read_exact(self, buffer.as_mut())?;
        Ok(buffer)
    }
}

impl VortexRead for Cursor<&[u8]> {
    async fn read_into(&mut self, mut buffer: BytesMut) -> io::Result<BytesMut> {
        Read::read_exact(self, buffer.as_mut())?;
        Ok(buffer)
    }
}

impl VortexReadAt for Vec<u8> {
    fn read_at_into(
        &mut self,
        pos: u64,
        buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>> {
        VortexReadAt::read_at_into(self.as_mut_slice(), pos, buffer)
    }
}

impl VortexReadAt for [u8] {
    fn read_at_into(
        &mut self,
        pos: u64,
        mut buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>> {
        buffer.copy_from_slice(&self[pos as usize..]);
        ready(Ok(buffer))
    }
}
