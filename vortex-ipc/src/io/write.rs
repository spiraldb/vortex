use std::future::{ready, Future};
use std::io;

use vortex_buffer::io::IoBuf;

pub trait VortexWrite {
    fn write_all<B: IoBuf>(&mut self, buffer: B) -> impl Future<Output = io::Result<B>>;
    fn flush(&mut self) -> impl Future<Output = io::Result<()>>;
    fn shutdown(&mut self) -> impl Future<Output = io::Result<()>>;
}

impl VortexWrite for Vec<u8> {
    fn write_all<B: IoBuf>(&mut self, buffer: B) -> impl Future<Output = io::Result<B>> {
        let slice =
            unsafe { std::slice::from_raw_parts::<u8>(buffer.read_ptr(), buffer.bytes_init()) };
        self.copy_from_slice(slice);
        ready(Ok(buffer))
    }

    fn flush(&mut self) -> impl Future<Output = io::Result<()>> {
        ready(Ok(()))
    }

    fn shutdown(&mut self) -> impl Future<Output = io::Result<()>> {
        ready(Ok(()))
    }
}
