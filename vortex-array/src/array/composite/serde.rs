use crate::array::composite::{CompositeArray, CompositeEncoding};
use crate::array::{Array, ArrayRef};
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for CompositeArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        // TODO(ngates): just write the ID and metadata?
        ctx.dtype(self.dtype())?;
        ctx.write(self.underlying())
    }
}

impl EncodingSerde for CompositeEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let DType::Composite(id, underlying, metadata) = ctx.dtype()? else {
            panic!("Invalid DType")
        };
        Ok(CompositeArray::new(id, metadata, ctx.with_schema(&underlying).read()?).boxed())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::array::composite::CompositeArray;
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::Array;
    use crate::dtype::Metadata;
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = CompositeArray::new(
            Arc::new("test".into()),
            Metadata::default(),
            vec![7u8, 37, 71, 97].into(),
        );

        let read_arr = roundtrip_array(arr.as_ref()).unwrap();

        assert_eq!(
            arr.underlying().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_composite()
                .underlying()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(arr.dtype(), read_arr.dtype());
    }
}
