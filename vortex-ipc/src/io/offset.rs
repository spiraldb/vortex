use std::future::Future;
use std::io;

use bytes::BytesMut;

use crate::io::{VortexRead, VortexReadAt};

/// An adapter that offsets all reads by a fixed amount.
pub struct OffsetReadAt<R> {
    read: R,
    offset: u64,
}

impl<R: VortexReadAt> OffsetReadAt<R> {
    pub fn new(read: R, offset: u64) -> Self {
        Self { read, offset }
    }
}

impl<R: VortexReadAt> VortexReadAt for OffsetReadAt<R> {
    fn read_at_into(
        &mut self,
        pos: u64,
        buffer: BytesMut,
    ) -> impl Future<Output = io::Result<BytesMut>> {
        self.read.read_at_into(pos + self.offset, buffer)
    }

    fn performance_hint(&self) -> usize {
        self.read.performance_hint()
    }
}

impl<R: VortexReadAt> VortexRead for OffsetReadAt<R> {
    async fn read_into(&mut self, buffer: BytesMut) -> io::Result<BytesMut> {
        let buffer_len = buffer.len() as u64;
        let res = self.read.read_at_into(self.offset, buffer).await?;
        self.offset += buffer_len;
        Ok(res)
    }
}
