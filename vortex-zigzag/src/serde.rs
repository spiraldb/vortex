use std::io;
use std::io::ErrorKind;

use vortex::array::{Array, ArrayRef};
use vortex::dtype::{DType, Signedness};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{ZigZagArray, ZigZagEncoding};

impl ArraySerde for ZigZagArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for ZigZagEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let encoded_dtype = match ctx.schema() {
            DType::Int(w, Signedness::Signed, n) => DType::Int(*w, Signedness::Unsigned, *n),
            _ => {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "Invalid zigzag encoded dtype, not an signed integer",
                ));
            }
        };
        let encoded = ctx.with_schema(&encoded_dtype).read()?;
        Ok(ZigZagArray::new(encoded).boxed())
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::compress::zigzag_encode;
    use crate::downcast::DowncastZigzag;

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
        let arr = zigzag_encode(&PrimitiveArray::from_vec(vec![-7i64, -13, 17, 23])).unwrap();
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        let read_zigzag = read_arr.as_zigzag();
        assert_eq!(
            arr.encoded().as_primitive().buffer().typed_data::<u8>(),
            read_zigzag
                .encoded()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );
    }
}
