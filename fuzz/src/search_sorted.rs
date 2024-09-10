use std::cmp::Ordering;

use vortex::accessor::ArrayAccessor;
use vortex::compute::{IndexOrd, Len, SearchResult, SearchSorted, SearchSortedSide};
use vortex::validity::ArrayValidity;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_buffer::{Buffer, BufferString};
use vortex_dtype::{match_each_native_ptype, DType};
use vortex_scalar::Scalar;

struct SearchNullableSlice<T>(Vec<Option<T>>);

impl<T: PartialOrd> IndexOrd<Option<T>> for SearchNullableSlice<T> {
    fn index_cmp(&self, idx: usize, elem: &Option<T>) -> Option<Ordering> {
        match elem {
            None => unreachable!("Can't search for None"),
            Some(v) =>
            // SAFETY: Used in search_sorted_by same as the standard library. The search_sorted ensures idx is in bounds
            {
                match unsafe { self.0.get_unchecked(idx) } {
                    None => Some(Ordering::Greater),
                    Some(i) => i.partial_cmp(v),
                }
            }
        }
    }
}

impl<T> Len for SearchNullableSlice<T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

pub fn search_sorted_canonical_array(
    array: &Array,
    scalar: &Scalar,
    side: SearchSortedSide,
) -> SearchResult {
    match array.dtype() {
        DType::Bool(_) => {
            let bool_array = array.clone().into_bool().unwrap();
            let opt_values = bool_array
                .boolean_buffer()
                .iter()
                .zip(
                    bool_array
                        .logical_validity()
                        .into_array()
                        .into_bool()
                        .unwrap()
                        .boolean_buffer()
                        .iter(),
                )
                .map(|(b, v)| v.then_some(b))
                .collect::<Vec<_>>();
            let to_find = scalar.try_into().unwrap();
            SearchNullableSlice(opt_values).search_sorted(&Some(to_find), side)
        }
        DType::Primitive(p, _) => match_each_native_ptype!(p, |$P| {
            let primitive_array = array.clone().into_primitive().unwrap();
            let opt_values = primitive_array
                .maybe_null_slice::<$P>()
                .iter()
                .copied()
                .zip(
                    primitive_array
                        .logical_validity()
                        .into_array()
                        .into_bool()
                        .unwrap()
                        .boolean_buffer()
                        .iter(),
                )
                            .map(|(b, v)| v.then_some(b))
                .collect::<Vec<_>>();
            let to_find: $P = scalar.try_into().unwrap();
            SearchNullableSlice(opt_values).search_sorted(&Some(to_find), side)
        }),
        DType::Utf8(_) | DType::Binary(_) => {
            let utf8 = array.clone().into_varbin().unwrap();
            let opt_values = utf8
                .with_iterator(|iter| iter.map(|v| v.map(|u| u.to_vec())).collect::<Vec<_>>())
                .unwrap();
            let to_find = if matches!(array.dtype(), DType::Utf8(_)) {
                BufferString::try_from(scalar)
                    .unwrap()
                    .as_str()
                    .as_bytes()
                    .to_vec()
            } else {
                Buffer::try_from(scalar).unwrap().to_vec()
            };
            SearchNullableSlice(opt_values).search_sorted(&Some(to_find), side)
        }
        _ => unreachable!("Not a canonical array"),
    }
}
