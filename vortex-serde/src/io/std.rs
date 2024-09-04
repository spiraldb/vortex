use std::future::{ready, Future};
use std::io;
use std::io::Write;
use std::os::unix::prelude::FileExt;

use bytes::BytesMut;
use vortex_buffer::io_buf::IoBuf;

use crate::io::{VortexReadAt, VortexWrite};

pub struct StdFile(std::fs::File);

impl VortexWrite for StdFile {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        self.0.write_all(buffer.as_slice())?;
        Ok(buffer)
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.0.flush()?;
        Ok(())
    }

    fn shutdown(&mut self) -> impl Future<Output = io::Result<()>> {
        ready(Ok(()))
    }
}

impl VortexReadAt for StdFile {
    async fn read_at_into(&self, pos: u64, mut buffer: BytesMut) -> io::Result<BytesMut> {
        self.0.read_exact_at(buffer.as_mut(), pos)?;
        Ok(buffer)
    }

    async fn size(&self) -> u64 {
        self.0.metadata().unwrap().len()
    }
}
