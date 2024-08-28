use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
use vortex::ArrayDType;
use vortex_error::{vortex_panic, VortexResult};
use vortex_scalar::Scalar;

use crate::{unpack_single, BitPackedArray};

impl ScalarAtFn for BitPackedArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if let Some(patches) = self.patches() {
            // NB: All non-null values are considered patches
            if self.bit_width() == 0 || patches.with_dyn(|a| a.is_valid(index)) {
                return scalar_at_unchecked(&patches, index).cast(self.dtype());
            }
        }

        unpack_single(self, index)?.cast(self.dtype())
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        self.scalar_at(index).unwrap_or_else(|err| vortex_panic!(err))
    }
}

#[cfg(test)]
mod test {
    use vortex::array::{PrimitiveArray, SparseArray};
    use vortex::compute::unary::scalar_at;
    use vortex::validity::Validity;
    use vortex::IntoArray;
    use vortex_dtype::{DType, Nullability, PType};
    use vortex_scalar::Scalar;

    use crate::BitPackedArray;

    #[test]
    fn invalid_patches() {
        let packed_array = BitPackedArray::try_new(
            PrimitiveArray::from(vec![0u32; 32]).into_array(),
            Validity::AllInvalid,
            Some(
                SparseArray::try_new(
                    PrimitiveArray::from(vec![1u64]).into_array(),
                    PrimitiveArray::from_vec(vec![999u32], Validity::AllValid).into_array(),
                    8,
                    Scalar::null(DType::Primitive(PType::U32, Nullability::Nullable)),
                )
                .unwrap()
                .into_array(),
            ),
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
