use std::ops::Shr;

use num_traits::WrappingSub;
use vortex::compute::unary::{scalar_at, scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{
    search_sorted, slice, take, ArrayCompute, SearchResult, SearchSortedFn, SearchSortedSide,
    SliceFn, TakeFn,
};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::{match_each_integer_ptype, NativePType};
use vortex_error::{VortexError, VortexResult};
use vortex_scalar::{PValue, PrimitiveScalar, Scalar};

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
        Ok(self.scalar_at_unchecked(index))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        let encoded_scalar =
            scalar_at_unchecked(&self.encoded(), index).reinterpret_cast(self.ptype());
        let encoded = PrimitiveScalar::try_from(&encoded_scalar).unwrap();
        let reference = PrimitiveScalar::try_from(self.reference()).unwrap();

        match_each_integer_ptype!(encoded.ptype(), |$P| {
            use num_traits::WrappingAdd;
            encoded.typed_value::<$P>().map(|v| (v << self.shift()).wrapping_add(reference.typed_value::<$P>().unwrap()))
                    .map(|v| Scalar::primitive::<$P>(v, encoded.dtype().nullability()))
                    .unwrap_or_else(|| Scalar::null(encoded.dtype().clone()))
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
            search_sorted_typed::<$P>(self, value, side)
        })
    }
}

fn search_sorted_typed<T>(
    array: &FoRArray,
    value: &Scalar,
    side: SearchSortedSide,
) -> VortexResult<SearchResult>
where
    T: NativePType
        + for<'a> TryFrom<&'a Scalar, Error = VortexError>
        + Shr<u8, Output = T>
        + WrappingSub
        + Into<PValue>,
{
    let min: T = array.reference().try_into()?;
    let shifted_min = min >> array.shift();
    let unwrapped_value: T = value.cast(array.dtype())?.as_ref().try_into()?;
    let shifted_value: T = unwrapped_value >> array.shift();
    // Make sure that smaller values are still smaller and not larger than (which they would be after wrapping_sub)
    if shifted_value < shifted_min {
        return Ok(SearchResult::NotFound(0));
    }

    let translated_scalar = Scalar::primitive(
        shifted_value.wrapping_sub(&shifted_min),
        value.dtype().nullability(),
    )
    .reinterpret_cast(array.ptype().to_unsigned());

    // When the values in the array are shifted, not all values in the domain are representable in the compressed
    // space. Multiple different search values can translate to same value in the compressed space. In order to
    // ensure that we found a value we were looking for we need to check if the value at the found index is equal
    // to the searched value.
    match search_sorted(&array.encoded(), translated_scalar, side)? {
        SearchResult::Found(i) => {
            let found_scalar = scalar_at(array.array(), i)?;
            if &found_scalar == value {
                Ok(SearchResult::Found(i))
            } else {
                // This would only ever be +1 since the value originally looked for is larger than the found (translated) value
                Ok(SearchResult::NotFound(i + 1))
            }
        }
        SearchResult::NotFound(i) => Ok(SearchResult::NotFound(i)),
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
        let for_arr = for_compress(&PrimitiveArray::from(vec![1100, 1500, 1900])).unwrap();
        assert_eq!(scalar_at(&for_arr, 0).unwrap(), 1100.into());
        assert_eq!(scalar_at(&for_arr, 1).unwrap(), 1500.into());
        assert_eq!(scalar_at(&for_arr, 2).unwrap(), 1900.into());
    }

    #[test]
    fn for_search() {
        let for_arr = for_compress(&PrimitiveArray::from(vec![1100, 1500, 1900])).unwrap();
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

    #[test]
    fn search_with_shift_notfound() {
        let for_arr = for_compress(&PrimitiveArray::from(vec![62, 114])).unwrap();
        assert_eq!(
            search_sorted(&for_arr, 63, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(1)
        );
        let for_arr = for_compress(&PrimitiveArray::from(vec![62, 114])).unwrap();
        assert_eq!(
            search_sorted(&for_arr, 61, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(0)
        );
        let for_arr = for_compress(&PrimitiveArray::from(vec![62, 114])).unwrap();
        assert_eq!(
            search_sorted(&for_arr, 113, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(1)
        );
        assert_eq!(
            search_sorted(&for_arr, 115, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(2)
        );
    }
}
