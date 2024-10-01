use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::compute::{search_sorted, SearchResult, SearchSortedSide};
use vortex::ArrayDType;
use vortex_error::{VortexResult, VortexUnwrap as _};
use vortex_scalar::Scalar;

use crate::{unpack_single, BitPackedArray};

impl ScalarAtFn for BitPackedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if let Some((indices, values, _)) = self._patches() {
            // NB: All non-null values are considered patches
            match search_sorted(&indices, index, SearchSortedSide::Left)? {
                SearchResult::Found(patches_index) => {
                    return scalar_at(&values, patches_index)?.cast(self.dtype());
                }
                SearchResult::NotFound(_) => {}
            }
        }

        unpack_single(self, index)?.cast(self.dtype())
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        self.scalar_at(index).vortex_unwrap()
    }
}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;
    use vortex::compute::unary::scalar_at;
    use vortex::validity::Validity;
    use vortex::IntoArray;
    use vortex_buffer::Buffer;
    use vortex_dtype::{DType, Nullability, PType};
    use vortex_scalar::{Scalar, ScalarValue};

    use crate::BitPackedArray;

    #[test]
    fn invalid_patches() {
        let packed_array = BitPackedArray::try_new(
            Buffer::from(vec![0u8; 128]),
            PType::U32,
            Validity::AllInvalid,
            Some((
                PrimitiveArray::from(vec![1u64]).into_array(),
                PrimitiveArray::from_vec(vec![999u32], Validity::NonNullable).into_array(),
                0,
            )),
            1,
            8,
        )
        .unwrap()
        .into_array();
        assert_eq!(
            scalar_at(&packed_array, 1).unwrap(),
            Scalar::null(DType::Primitive(PType::U32, Nullability::Nullable))
        );
    }
}
