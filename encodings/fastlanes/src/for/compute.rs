use std::ops::{AddAssign, Shl, Shr};

use num_traits::{WrappingAdd, WrappingSub};
use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
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
        + Shl<u8, Output = T>
        + WrappingSub
        + WrappingAdd
        + AddAssign
        + Into<PValue>,
{
    let min: T = array.reference().try_into()?;
    let primitive_value: T = value.cast(array.dtype())?.as_ref().try_into()?;
    // Make sure that smaller values are still smaller and not larger than (which they would be after wrapping_sub)
    if primitive_value < min {
        return Ok(SearchResult::NotFound(0));
    }

    // When the values in the array are shifted, not all values in the domain are representable in the compressed
    // space. Multiple different search values can translate to same value in the compressed space.
    //
    // For values that are not representable in the compressed array we know they wouldn't be found in the array
    // in order to find index they would be inserted at we search for next value in the compressed space
    let mut translated_value = primitive_value.wrapping_sub(&min) >> array.shift();
    let mut non_representable = false;
    if (translated_value << array.shift()).wrapping_add(&min) != primitive_value {
        translated_value += T::from(1).unwrap();
        non_representable = true;
    }

    let translated_scalar = Scalar::primitive(translated_value, value.dtype().nullability())
        .reinterpret_cast(array.ptype().to_unsigned());

    Ok(
        match search_sorted(&array.encoded(), translated_scalar, side)? {
            SearchResult::Found(i) => {
                if non_representable {
                    // If we are searching from the right and our value has not been representable we might have hit
                    // the next value and need to shift the result
                    SearchResult::NotFound(if matches!(side, SearchSortedSide::Right) {
                        i - 1
                    } else {
                        i
                    })
                } else {
                    SearchResult::Found(i)
                }
            }
            s @ SearchResult::NotFound(_) => s,
        },
    )
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

    #[test]
    fn search_with_shift_notfound_repeated() {
        let for_arr = for_compress(&PrimitiveArray::from(vec![62, 62, 114, 114])).unwrap();
        assert_eq!(
            search_sorted(&for_arr, 63, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(2)
        );
        assert_eq!(
            search_sorted(&for_arr, 113, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(2)
        );
        assert_eq!(
            search_sorted(&for_arr, 115, SearchSortedSide::Left).unwrap(),
            SearchResult::NotFound(4)
        );
    }

    #[test]
    fn search_with_shift_right() {
        let for_arr = for_compress(&PrimitiveArray::from(vec![8, 40, 48, 50, 58])).unwrap();
        assert_eq!(
            search_sorted(&for_arr, 39, SearchSortedSide::Right).unwrap(),
            SearchResult::NotFound(1)
        );
    }
}
