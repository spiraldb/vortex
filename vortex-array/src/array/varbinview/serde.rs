use vortex_error::VortexResult;

use crate::array::varbinview::{VarBinViewArray, VarBinViewEncoding};
use crate::array::{Array, ArrayRef};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use crate::validity::OwnedValidity;

impl ArraySerde for VarBinViewArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_validity(self.validity())?;
        ctx.write(self.views())?;
        ctx.write_usize(self.data().len())?;
        for d in self.data() {
            ctx.write(d.as_ref())?;
        }
        Ok(())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        Ok(None)
    }
}

impl EncodingSerde for VarBinViewEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let validity = ctx.read_validity()?;
        let views = ctx.bytes().read()?;
        let num_data = ctx.read_usize()?;
        let mut data_bufs = Vec::<ArrayRef>::with_capacity(num_data);
        for _ in 0..num_data {
            data_bufs.push(ctx.bytes().read()?);
        }
        Ok(
            VarBinViewArray::try_new(views, data_bufs, ctx.schema().clone(), validity)
                .unwrap()
                .into_array(),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::varbinview::VarBinViewArray;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = VarBinViewArray::from(vec!["hello world", "hello world this is a long string"]);

        let read_arr = roundtrip_array(&arr).unwrap();

        assert_eq!(
            arr.views().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_varbinview()
                .views()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.data()[0].as_primitive().buffer().typed_data::<u8>(),
            read_arr.as_varbinview().data()[0]
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );
    }
}
