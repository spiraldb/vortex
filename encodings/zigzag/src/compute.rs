use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{slice, ArrayCompute, SliceFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::match_each_unsigned_integer_ptype;
use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::{PrimitiveScalar, Scalar};
use zigzag::{ZigZag as ExternalZigZag, ZigZag};

use crate::ZigZagArray;

impl ArrayCompute for ZigZagArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

impl ScalarAtFn for ZigZagArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let scalar = scalar_at_unchecked(&self.encoded(), index);
        if scalar.is_null() {
            return Ok(scalar.reinterpret_cast(self.ptype()));
        }

        let pscalar = PrimitiveScalar::try_from(&scalar)?;
        match_each_unsigned_integer_ptype!(pscalar.ptype(), |$P| {
            Ok(Scalar::primitive(
                <<$P as ZigZagEncoded>::Int>::decode(pscalar.typed_value::<$P>().ok_or_else(|| {
                    vortex_err!(
                        "Cannot decode provided scalar: expected u8, got ptype {}",
                        pscalar.ptype()
                    )
                })?),
                self.dtype().nullability(),
            ))
        })
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).unwrap()
    }
}

trait ZigZagEncoded {
    type Int: ZigZag;
}

impl ZigZagEncoded for u8 {
    type Int = i8;
}

impl ZigZagEncoded for u16 {
    type Int = i16;
}

impl ZigZagEncoded for u32 {
    type Int = i32;
}

impl ZigZagEncoded for u64 {
    type Int = i64;
}

impl SliceFn for ZigZagArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(slice(&self.encoded(), start, stop)?)?.into_array())
    }
}

#[cfg(test)]
mod tests {
    use vortex::array::PrimitiveArray;
    use vortex::compute::unary::scalar_at;
    use vortex::compute::{search_sorted, SearchResult, SearchSortedSide};
    use vortex::validity::Validity;
    use vortex::IntoArray;
    use vortex_dtype::Nullability;
    use vortex_scalar::Scalar;

    use crate::ZigZagArray;

    #[test]
    pub fn search_sorted_uncompressed() {
        let zigzag =
            ZigZagArray::encode(&PrimitiveArray::from(vec![-189, -160, 1]).into_array()).unwrap();
        assert_eq!(
            search_sorted(&zigzag, -169, SearchSortedSide::Right).unwrap(),
            SearchResult::NotFound(1)
        );
    }

    #[test]
    pub fn nullable_scalar_at() {
        let zigzag = ZigZagArray::encode(
            &PrimitiveArray::from_vec(vec![-189, -160, 1], Validity::AllValid).into_array(),
        )
        .unwrap();
        assert_eq!(
            scalar_at(&zigzag, 1).unwrap(),
            Scalar::primitive(-160, Nullability::Nullable)
        );
    }
}
