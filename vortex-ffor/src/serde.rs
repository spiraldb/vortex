use std::io;

use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{FFORArray, FFoREncoding};

impl ArraySerde for FFORArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        ctx.write_fixed_slice([self.num_bits()])?;
        ctx.scalar(self.min_val())?;
        if let Some(v) = self.validity() {
            ctx.write(v.as_ref())?;
        }

        ctx.write_option_tag(self.patches().is_some())?;
        if let Some(p) = self.patches() {
            ctx.dtype(p.dtype())?;
            ctx.write(p.as_ref())?;
        }
        // TODO(robert): Stop writing this
        ctx.dtype(self.encoded().dtype())?;
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for FFoREncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let num_bits = ctx.read_nbytes::<1>()?[0];
        let min_val = ctx.scalar()?;
        let validity = if ctx.schema().is_nullable() {
            Some(ctx.validity().read()?)
        } else {
            None
        };
        let patches_tag = ctx.read_nbytes::<1>()?[0];
        let patches = if patches_tag == 0x01 {
            let patches_dtype = ctx.dtype()?;
            Some(ctx.with_schema(&patches_dtype).read()?)
        } else {
            None
        };
        let encoded_dtype = ctx.dtype()?;
        let encoded = ctx.with_schema(&encoded_dtype).read()?;
        Ok(FFORArray::new(encoded, validity, patches, min_val, num_bits, len).boxed())
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::{Array, ArrayRef};
    use vortex::serde::{ReadCtx, WriteCtx};

    use crate::compress::ffor_encode;
    use crate::downcast::DowncastFFOR;

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
        let arr = ffor_encode(&PrimitiveArray::from(vec![-7i64, -13, 17, 23]));
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.encoded().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_ffor()
                .encoded()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(arr.min_val(), read_arr.as_ffor().min_val());

        assert_eq!(
            arr.patches()
                .unwrap()
                .as_sparse()
                .values()
                .as_primitive()
                .buffer()
                .typed_data::<i64>(),
            read_arr
                .as_ffor()
                .patches()
                .unwrap()
                .as_sparse()
                .values()
                .as_primitive()
                .buffer()
                .typed_data::<i64>()
        );
    }
}
