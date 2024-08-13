use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::compute::{
    search_sorted, slice, take, ArrayCompute, SearchResult, SearchSortedFn, SearchSortedSide,
    SliceFn, TakeFn,
};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::{PrimitiveScalar, Scalar, ScalarValue};

use crate::FoRArray;

impl ArrayCompute for FoRArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl TakeFn for FoRArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Self::try_new(
            take(&self.encoded(), indices)?,
            self.reference().clone(),
            self.shift(),
        )
        .map(|a| a.into_array())
    }
}

impl ScalarAtFn for FoRArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let encoded_scalar = scalar_at(&self.encoded(), index)?.reinterpret_cast(self.ptype());
        let encoded = PrimitiveScalar::try_from(&encoded_scalar)?;
        let reference = PrimitiveScalar::try_from(self.reference())?;

        if encoded.ptype() != reference.ptype() {
            vortex_bail!("Reference and encoded values had different dtypes");
        }

        match_each_integer_ptype!(encoded.ptype(), |$P| {
            use num_traits::WrappingAdd;
            Ok(encoded.typed_value::<$P>().map(|v| (v << self.shift()).wrapping_add(reference.typed_value::<$P>().unwrap()))
                    .map(|v| Scalar::primitive::<$P>(v, encoded.dtype().nullability()))
                    .unwrap_or_else(|| Scalar::new(encoded.dtype().clone(), ScalarValue::Null)))
        })
    }
}

impl SliceFn for FoRArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Self::try_new(
            slice(&self.encoded(), start, stop)?,
            self.reference().clone(),
            self.shift(),
        )
        .map(|a| a.into_array())
    }
}

impl SearchSortedFn for FoRArray {
    fn search_sorted(&self, value: &Scalar, side: SearchSortedSide) -> VortexResult<SearchResult> {
        match_each_integer_ptype!(self.ptype(), |$P| {
            let min: $P = self.reference().try_into().unwrap();
            let shifted_min = min >> self.shift();
            let unwrapped_value: $P = value.cast(self.dtype())?.try_into().unwrap();
            let shifted_value: $P = unwrapped_value >> self.shift();
            // Make sure that smaller values are still smaller and not larger than (which they would be after wrapping_sub)
            if shifted_value < shifted_min {
                return Ok(SearchResult::NotFound(0));
            }

            let translated_scalar = Scalar::primitive(
                shifted_value.wrapping_sub(shifted_min),
                value.dtype().nullability(),
            )
            .reinterpret_cast(self.ptype().to_unsigned());
            search_sorted(&self.encoded(), translated_scalar, side)
        })
    }
}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;
    use vortex::compute::unary::scalar_at;
    use vortex::compute::{search_sorted, SearchResult, SearchSortedSide};

    use crate::for_compress;

    #[test]
    fn for_scalar_at() {
        let for_arr = for_compress(PrimitiveArray::from(vec![1100, 1500, 1900]).array()).unwrap();
        assert_eq!(scalar_at(&for_arr, 0).unwrap(), 1100.into());
        assert_eq!(scalar_at(&for_arr, 1).unwrap(), 1500.into());
        assert_eq!(scalar_at(&for_arr, 2).unwrap(), 1900.into());
    }

    #[test]
    fn for_search() {
        let for_arr = for_compress(PrimitiveArray::from(vec![1100, 1500, 1900]).array()).unwrap();
        assert_eq!(
            search_sorted(&for_arr, 1500, SearchSortedSide::Left).unwrap(),
            SearchResult::Found(1)
        );
        assert_eq!(
            search_sorted(&for_arr, 2000, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(3)
        );
        assert_eq!(
            search_sorted(&for_arr, 1000, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(0)
        );
    }
}
