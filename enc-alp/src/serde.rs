use std::io;
use std::io::ErrorKind;

use codecz::alp::ALPExponents;
use enc::array::{Array, ArrayRef};
use enc::dtype::{DType, FloatWidth, Signedness};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{ALPArray, ALPEncoding};

impl ArraySerde for ALPArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_option_tag(self.patches().is_some())?;
        if let Some(p) = self.patches() {
            ctx.write(p.as_ref())?;
        }
        ctx.write_fixed_slice([self.exponents().e, self.exponents().f])?;
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for ALPEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let patches_tag = ctx.read_nbytes::<1>()?[0];
        let patches = if patches_tag == 0x01 {
            Some(ctx.read()?)
        } else {
            None
        };
        let exponents = ctx.read_nbytes::<2>()?;
        let encoded_dtype = match ctx.schema() {
            DType::Float(width, nullability) => match width {
                FloatWidth::_32 => DType::Int(32.into(), Signedness::Signed, *nullability),
                FloatWidth::_64 => DType::Int(64.into(), Signedness::Signed, *nullability),
                _ => return Err(io::Error::new(ErrorKind::InvalidData, "invalid dtype")),
            },
            _ => return Err(io::Error::new(ErrorKind::InvalidData, "invalid dtype")),
        };
        let encoded = ctx.with_schema(&encoded_dtype).read()?;
        Ok(ALPArray::new(
            encoded,
            ALPExponents {
                e: exponents[0],
                f: exponents[1],
            },
            patches,
        )
        .boxed())
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use enc::array::downcast::DowncastArrayBuiltin;
    use enc::array::primitive::PrimitiveArray;
    use enc::array::{Array, ArrayRef};
    use enc::serde::{ReadCtx, WriteCtx};

    use crate::compress::alp_encode;
    use crate::downcast::DowncastALP;

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
        let arr = alp_encode(&PrimitiveArray::from_vec(vec![
            0.00001f64,
            0.0004f64,
            1000000.0f64,
            0.33f64,
        ]));
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        let read_alp = read_arr.as_alp();
        assert_eq!(
            arr.encoded().as_primitive().buffer().typed_data::<i8>(),
            read_alp
                .encoded()
                .as_primitive()
                .buffer()
                .typed_data::<i8>()
        );

        assert_eq!(arr.exponents().e, read_alp.exponents().e);
        assert_eq!(arr.exponents().f, read_alp.exponents().f);
    }
}
