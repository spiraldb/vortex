use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::array::chunked::ChunkedArray;
use crate::compute::unary::{scalar_at, try_cast, CastFn, ScalarAtFn, SubtractScalarFn};
use crate::compute::{ArrayCompute, SliceFn, TakeFn};
use crate::{Array, IntoArray};

mod slice;
mod take;

impl ArrayCompute for ChunkedArray {
    fn cast(&self) -> Option<&dyn CastFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn subtract_scalar(&self) -> Option<&dyn SubtractScalarFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for ChunkedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let (chunk_index, chunk_offset) = self.find_chunk_idx(index);
        scalar_at(&self.chunk(chunk_index).ok_or_else(|| vortex_err!(OutOfBounds: chunk_index, 0, self.nchunks()))?, chunk_offset)
    }
}

impl CastFn for ChunkedArray {
    fn cast(&self, dtype: &DType) -> VortexResult<Array> {
        let mut cast_chunks = Vec::new();
        for chunk in self.chunks() {
            cast_chunks.push(try_cast(&chunk, dtype)?);
        }

        Ok(ChunkedArray::try_new(cast_chunks, dtype.clone())?.into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability, PType};

    use crate::array::chunked::ChunkedArray;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute::unary::try_cast;
    use crate::validity::Validity;
    use crate::{IntoArray, IntoArrayVariant};

    #[test]
    fn test_cast_chunked() {
        let arr0 = PrimitiveArray::from_vec(vec![0u32, 1], Validity::NonNullable).into_array();
        let arr1 = PrimitiveArray::from_vec(vec![2u32, 3], Validity::NonNullable).into_array();

        let chunked = ChunkedArray::try_new(
            vec![arr0, arr1],
            DType::Primitive(PType::U32, Nullability::NonNullable),
        )
        .unwrap()
        .into_array();

        // Two levels of chunking, just to be fancy.
        let root = ChunkedArray::try_new(
            vec![chunked],
            DType::Primitive(PType::U32, Nullability::NonNullable),
        )
        .unwrap()
        .into_array();

        assert_eq!(
            try_cast(
                &root,
                &DType::Primitive(PType::U64, Nullability::NonNullable)
            )
            .unwrap()
            .into_primitive()
            .unwrap()
            .into_maybe_null_slice::<u64>(),
            vec![0u64, 1, 2, 3],
        );
    }
}
