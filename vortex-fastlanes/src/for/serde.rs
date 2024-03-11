use std::io;

use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{FoRArray, FoREncoding};

impl ArraySerde for FoRArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.scalar(self.reference())?;
        ctx.write_usize(self.shift() as usize)?;
        ctx.write(self.child())
    }
}

impl EncodingSerde for FoREncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let reference = ctx.scalar()?;
        let shift = ctx.read_usize()? as u8;
        let child = ctx.read()?;
        Ok(FoRArray::try_new(child, reference, shift).unwrap().boxed())
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use vortex::array::{Array, ArrayRef};
    use vortex::scalar::Scalar;
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::FoRArray;

    fn roundtrip_array(array: &dyn Array) -> io::Result<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }

    #[test]
    fn roundtrip() {
        let arr = FoRArray::try_new(
            vec![-7i64, -13, 17, 23].into(),
            <i64 as Into<Scalar>>::into(-7i64),
        )
        .unwrap();
        roundtrip_array(arr.as_ref()).unwrap();
    }
}
