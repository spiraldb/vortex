use std::io;

use bytes::BytesMut;

use vortex_error::vortex_err;

pub trait VortexSyncRead {
    fn read_into(&mut self, buffer: BytesMut) -> io::Result<BytesMut>;
}

impl VortexSyncRead for BytesMut {
    fn read_into(&mut self, buffer: BytesMut) -> io::Result<BytesMut> {
        if buffer.len() > self.len() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                vortex_err!("unexpected eof"),
            ))
        } else {
            Ok(self.split_to(buffer.len()))
        }
    }
}
