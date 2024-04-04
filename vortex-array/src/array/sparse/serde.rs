use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::{Array, ArrayRef, OwnedArray};
use crate::scalar::Scalar;
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for SparseArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_usize(self.len())?;
        // TODO(robert): Rewrite indices and don't store offset
        ctx.write_usize(self.indices_offset())?;
        ctx.write(self.indices())?;
        ctx.write(self.values())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        // FIXME(ngates): use flatbuffer / serde.
        let mut vec = Vec::new();
        let mut ctx = WriteCtx::new(&mut vec);
        ctx.write_usize(self.len())?;
        // TODO(robert): Rewrite indices and don't store offset
        ctx.write_usize(self.indices_offset())?;
        Ok(Some(vec))
    }
}

impl EncodingSerde for SparseEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let len = ctx.read_usize()?;
        let offset = ctx.read_usize()?;
        let indices = ctx.with_schema(&DType::IDX).read()?;
        let values = ctx.read()?;
        let fill_type = values.dtype().clone().as_nullable();
        SparseArray::try_new_with_offset(
            indices,
            values,
            len,
            offset,
            // NB: We should deserialize the fill value from the source, but currently do not,
            // so everything that goes through this read path is nullable
            Scalar::null(&fill_type),
        )
        .map(|a| a.into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::SparseArray;
    use crate::array::IntoArray;
    use crate::array::OwnedArray;
    use crate::scalar::{NullScalar, Scalar};
    use crate::serde::test::roundtrip_array;

    #[test]
    fn roundtrip() {
        let arr = SparseArray::new(
            vec![7u64, 37, 71, 97].into_array(),
            PrimitiveArray::from_iter(vec![Some(0), None, Some(2), Some(42)]).into_array(),
            100,
            Scalar::Null(NullScalar::new()),
        );

        let read_arr = roundtrip_array(&arr).unwrap();

        assert_eq!(
            arr.indices().as_primitive().buffer().typed_data::<u8>(),
            read_arr
                .as_sparse()
                .indices()
                .as_primitive()
                .buffer()
                .typed_data::<u8>()
        );

        assert_eq!(
            arr.values().as_primitive().buffer().typed_data::<i32>(),
            read_arr
                .as_sparse()
                .values()
                .as_primitive()
                .buffer()
                .typed_data::<i32>()
        );
    }
}
