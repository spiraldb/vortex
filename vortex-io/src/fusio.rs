#![allow(unused)]

use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;

use bytes::BytesMut;
use fusio::dynamic::DynFile;
use fusio::{Error, IoBuf, IoBufMut};

use crate::read::{VortexRead, VortexReadAt};

impl VortexRead for Arc<dyn DynFile> {
    async fn read_into(&mut self, buffer: BytesMut) -> io::Result<BytesMut> {
        let buffer = unsafe { IoBufMut::slice_mut_unchecked(buffer, 0..) };
        let (result, buffer) = DynFile::read_exact(self, buffer).await;

        match result {
            Ok(()) => {}
            Err(e) => {
                return match e {
                    Error::Io(io_err) => Err(io_err),
                    other => Err(io::Error::new(ErrorKind::Other, Box::new(other))),
                }
            }
        }

        let buffer = unsafe { BytesMut::recover_from_buf_mut(buffer) };

        Ok(buffer)
    }
}

impl VortexReadAt for Arc<dyn DynFile> {
    async fn read_at_into(&mut self, pos: u64, buffer: BytesMut) -> io::Result<BytesMut> {
        // Perform a seek and then perform a read.
        self.seek(pos).await.map_err(|e| match e {
            Error::Io(io_err) => io_err,
            other => io::Error::new(ErrorKind::Other, other.into()),
        })?;

        self.read_into(buffer).await
    }

    async fn size(&self) -> u64 {
        DynFile::size(self).await.expect("reading size infallibly")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::BytesMut;
    use fusio::dynamic::DynFile;
    use fusio::IoBufMut;

    #[monoio::test]
    async fn test() {
        // Test fusio writer.
        // Create a monoio file, which impls the DynRead trait.

        std::fs::write("/tmp/a", b"hello mono").unwrap();

        let mono_file = monoio::fs::File::open("/tmp/a");
        let mut data: Arc<dyn DynFile> = Arc::new(mono_file);

        let mut slice = BytesMut::zeroed(10);
        let slice = unsafe { IoBufMut::slice_mut_unchecked(slice, 0usize..) };
        let (result, slice) = DynFile::read_exact(slice).await;
        let _ = result.unwrap();
        let slice = unsafe { BytesMut::recover_from_buf_mut(slice) };
        // Get back the read file
        assert_eq!(slice.as_ref(), b"hello mono");

        Ok(())
    }
}
