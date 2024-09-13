use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater};

use arrow_buffer::BooleanBuffer;
use vortex::accessor::ArrayAccessor;
use vortex::compute::{IndexOrd, Len, SearchResult, SearchSorted, SearchSortedSide};
use vortex::validity::ArrayValidity;
use vortex::variants::StructArrayTrait;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_buffer::{Buffer, BufferString};
use vortex_dtype::{match_each_native_ptype, DType};
use vortex_error::VortexExpect;
use vortex_scalar::{Scalar, StructScalar};

struct SearchNullableSlice<T>(Vec<Option<T>>);

impl<T: PartialOrd> IndexOrd<Option<T>> for SearchNullableSlice<T> {
    fn index_cmp(&self, idx: usize, elem: &Option<T>) -> Option<Ordering> {
        match elem {
            None => unreachable!("Can't search for None"),
            Some(v) => {
                // SAFETY: Used in search_sorted_by same as the standard library. The search_sorted ensures idx is in bounds
                match unsafe { self.0.get_unchecked(idx) } {
                    None => Some(Greater),
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

struct SearchEmptyStruct<'a>(&'a BooleanBuffer);

impl<'a> IndexOrd<Scalar> for SearchEmptyStruct<'a> {
    fn index_cmp(&self, idx: usize, _elem: &Scalar) -> Option<Ordering> {
        if !self.0.value(idx) {
            Some(Greater)
        } else {
            Some(Equal)
        }
    }
}

impl<'a> Len for SearchEmptyStruct<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

pub fn search_sorted_canonical_array(
    array: &Array,
    scalar: &Scalar,
    side: SearchSortedSide,
    additional_validity: Option<&BooleanBuffer>,
) -> SearchResult {
    match array.dtype() {
        DType::Bool(_) => {
            let bool_array = array.clone().into_bool().unwrap();
            let mut validity = bool_array
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer();
            if let Some(adv) = additional_validity {
                validity = &validity & adv;
            }
            let opt_values = bool_array
                .boolean_buffer()
                .iter()
                .zip(validity.iter())
                .map(|(b, v)| v.then_some(b))
                .collect::<Vec<_>>();
            let to_find = scalar.try_into().unwrap();
            SearchNullableSlice(opt_values).search_sorted(&Some(to_find), side)
        }
        DType::Primitive(p, _) => match_each_native_ptype!(p, |$P| {
            let primitive_array = array.clone().into_primitive().unwrap();
            let mut validity = primitive_array
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer();
            if let Some(adv) = additional_validity {
                validity = &validity & adv;
            }
            let opt_values = primitive_array
                .maybe_null_slice::<$P>()
                .iter()
                .copied()
                .zip(validity.iter())
                .map(|(b, v)| v.then_some(b))
                .collect::<Vec<_>>();
            let to_find: $P = scalar.try_into().unwrap();
            SearchNullableSlice(opt_values).search_sorted(&Some(to_find), side)
        }),
        DType::Utf8(_) | DType::Binary(_) => {
            let utf8 = array.clone().into_varbin().unwrap();
            let opt_values = utf8
                .with_iterator(|iter| {
                    if let Some(adv) = additional_validity {
                        iter.enumerate()
                            .map(|(i, v)| {
                                adv.value(i).then(|| v.map(|u| u.to_vec())).unwrap_or(None)
                            })
                            .collect::<Vec<_>>()
                    } else {
                        iter.map(|v| v.map(|u| u.to_vec())).collect::<Vec<_>>()
                    }
                })
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
        DType::Struct(..) => {
            let strct = array.clone().into_struct().unwrap();
            let struct_scalar: StructScalar = scalar.try_into().unwrap();
            let mut validity = strct
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer();
            if let Some(adv) = additional_validity {
                validity = &validity & adv;
            }
            if strct.nfields() == 0 {
                SearchEmptyStruct(&validity)
                    .search_sorted(&Scalar::r#struct(array.dtype().clone(), vec![]), side)
            } else {
                // This value will never be returned since there's at least one child
                let mut results = Vec::new();
                for (c, i) in strct.children().zip(0..strct.names().len()) {
                    let res = search_sorted_canonical_array(
                        &c,
                        &struct_scalar.field_by_idx(i).unwrap(),
                        side,
                        Some(&validity),
                    );
                    if let SearchResult::NotFound(u) = res {
                        return SearchResult::NotFound(u);
                    }
                    results.push(res.to_index());
                }
                SearchResult::Found(
                    results
                        .iter()
                        .max()
                        .copied()
                        .vortex_expect("there's at least one field"),
                )
            }
        }
        _ => unreachable!("Not a canonical array"),
    }
}
