#[cfg(test)]
pub mod test {
    use enc::array::{Array, ArrayRef};
    use enc::serde::{ReadCtx, WriteCtx};
    use std::io;

    pub fn roundtrip_array(array: &dyn Array) -> io::Result<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }
}
