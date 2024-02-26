use std::io;

use enc::array::{Array, ArrayRef};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{DictArray, DictEncoding};

impl ArraySerde for DictArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write(self.dict())?;
        // TODO(robert): Stop writing this
        ctx.dtype(self.codes().dtype())?;
        ctx.write(self.codes())
    }
}

impl EncodingSerde for DictEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let dict = ctx.read()?;
        let codes_dtype = ctx.dtype()?;
        let codes = ctx.with_schema(&codes_dtype).read()?;
        Ok(DictArray::new(codes, dict).boxed())
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use enc::array::downcast::DowncastArrayBuiltin;
    use enc::array::primitive::PrimitiveArray;
    use enc::array::{Array, ArrayRef};
    use enc::serde::{ReadCtx, WriteCtx};

    use crate::downcast::DowncastDict;
    use crate::DictArray;

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
        let arr = DictArray::new(
            PrimitiveArray::from_vec(vec![0u8, 0, 1, 2, 3]).boxed(),
            PrimitiveArray::from_vec(vec![-7i64, -13, 17, 23]).boxed(),
        );
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.codes().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_dict()
                .codes()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.dict().as_primitive().buffer().typed_data::<i64>(),
            read_arr
                .as_dict()
                .dict()
                .as_primitive()
                .buffer()
                .typed_data::<i64>()
        );
    }
}
