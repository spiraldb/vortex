use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter;
use std::ops::Range;

use libfuzzer_sys::arbitrary::Error::EmptyChoose;
use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
use vortex::accessor::ArrayAccessor;
use vortex::array::{BoolArray, PrimitiveArray, VarBinArray};
use vortex::compute::unary::scalar_at;
use vortex::compute::{IndexOrd, Len, SearchResult, SearchSorted, SearchSortedSide};
use vortex::validity::{ArrayValidity, Validity};
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_buffer::{Buffer, BufferString};
use vortex_dtype::{
    match_each_float_ptype, match_each_integer_ptype, match_each_native_ptype, DType,
};
use vortex_sampling_compressor::SamplingCompressor;
use vortex_scalar::arbitrary::random_scalar;
use vortex_scalar::Scalar;

#[derive(Debug)]
pub enum ExpectedValue {
    Array(Array),
    Search(SearchResult),
}

impl ExpectedValue {
    pub fn array(self) -> Array {
        match self {
            ExpectedValue::Array(array) => array,
            _ => panic!("expected array"),
        }
    }

    pub fn search(self) -> SearchResult {
        match self {
            ExpectedValue::Search(s) => s,
            _ => panic!("expected search"),
        }
    }
}

#[derive(Debug)]
pub struct FuzzArrayAction {
    pub array: Array,
    pub actions: Vec<(Action, ExpectedValue)>,
}

#[derive(Debug)]
pub enum Action {
    Compress(SamplingCompressor<'static>),
    Slice(Range<usize>),
    Take(Array),
    SearchSorted(Scalar, SearchSortedSide),
}

impl<'a> Arbitrary<'a> for FuzzArrayAction {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let array = Array::arbitrary(u)?;
        let mut current_array = array.clone();
        let mut actions = Vec::new();
        for _ in 0..u.int_in_range(1..=4)? {
            actions.push(match u.int_in_range(0..=3)? {
                0 => {
                    if actions
                        .last()
                        .map(|(l, _)| matches!(l, Action::Compress(_)))
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    (
                        Action::Compress(u.arbitrary()?),
                        ExpectedValue::Array(current_array.clone()),
                    )
                }
                1 => {
                    let start = u.choose_index(current_array.len())?;
                    let stop = u.int_in_range(start..=current_array.len())?;
                    current_array = slice_primitive_array(&current_array, start, stop);

                    (
                        Action::Slice(start..stop),
                        ExpectedValue::Array(current_array.clone()),
                    )
                }
                2 => {
                    if current_array.is_empty() {
                        return Err(EmptyChoose);
                    }

                    let indices = random_vec_in_range(u, 0, current_array.len() - 1)?;
                    current_array = take_primitive_array(&current_array, &indices);
                    let indices_array =
                        PrimitiveArray::from(indices.iter().map(|i| *i as u64).collect::<Vec<_>>())
                            .into();
                    let compressed = SamplingCompressor::default()
                        .compress(&indices_array, None)
                        .unwrap();
                    (
                        Action::Take(compressed.into_array()),
                        ExpectedValue::Array(current_array.clone()),
                    )
                }
                3 => {
                    let scalar = if u.arbitrary()? {
                        scalar_at(&current_array, u.choose_index(current_array.len())?).unwrap()
                    } else {
                        random_scalar(u, current_array.dtype())?
                    };

                    if scalar.is_null() {
                        return Err(EmptyChoose);
                    }

                    let sorted = sort_canonical_array(&current_array);

                    let side = if u.arbitrary()? {
                        SearchSortedSide::Left
                    } else {
                        SearchSortedSide::Right
                    };
                    (
                        Action::SearchSorted(scalar.clone(), side),
                        ExpectedValue::Search(search_values_in_primitive_array(
                            &sorted, &scalar, side,
                        )),
                    )
                }
                _ => unreachable!(),
            })
        }

        Ok(Self { array, actions })
    }
}

fn random_vec_in_range(u: &mut Unstructured<'_>, min: usize, max: usize) -> Result<Vec<usize>> {
    iter::from_fn(|| {
        if u.arbitrary().unwrap_or(false) {
            Some(u.int_in_range(min..=max))
        } else {
            None
        }
    })
    .collect::<Result<Vec<_>>>()
}

fn slice_primitive_array(array: &Array, start: usize, stop: usize) -> Array {
    match array.dtype() {
        DType::Bool(_) => {
            let bool_array = array.clone().into_bool().unwrap();
            let vec_values = bool_array.boolean_buffer().iter().collect::<Vec<_>>();
            let vec_validity = bool_array
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer()
                .iter()
                .collect::<Vec<_>>();
            BoolArray::from_vec(
                Vec::from(&vec_values[start..stop]),
                Validity::from(Vec::from(&vec_validity[start..stop])),
            )
            .into_array()
        }
        DType::Primitive(p, _) => match_each_native_ptype!(p, |$P| {
            let primitive_array = array.clone().into_primitive().unwrap();
            let vec_values = primitive_array
                .maybe_null_slice::<$P>()
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let vec_validity = primitive_array
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer()
                .iter()
                .collect::<Vec<_>>();
            PrimitiveArray::from_vec(
                Vec::from(&vec_values[start..stop]),
                Validity::from(Vec::from(&vec_validity[start..stop])),
            )
            .into_array()
        }),
        DType::Utf8(_) | DType::Binary(_) => {
            let utf8 = array.clone().into_varbin().unwrap();
            let values = utf8
                .with_iterator(|iter| iter.map(|v| v.map(|u| u.to_vec())).collect::<Vec<_>>())
                .unwrap();
            VarBinArray::from_iter(Vec::from(&values[start..stop]), array.dtype().clone())
                .into_array()
        }
        _ => unreachable!("Array::arbitrary will not generate other dtypes"),
    }
}

fn take_primitive_array(array: &Array, indices: &[usize]) -> Array {
    match array.dtype() {
        DType::Bool(_) => {
            let bool_array = array.clone().into_bool().unwrap();
            let vec_values = bool_array.boolean_buffer().iter().collect::<Vec<_>>();
            let vec_validity = bool_array
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer()
                .iter()
                .collect::<Vec<_>>();
            BoolArray::from_vec(
                indices.iter().map(|i| vec_values[*i]).collect(),
                Validity::from(indices.iter().map(|i| vec_validity[*i]).collect::<Vec<_>>()),
            )
            .into_array()
        }
        DType::Primitive(p, _) => match_each_native_ptype!(p, |$P| {
            let primitive_array = array.clone().into_primitive().unwrap();
            let vec_values = primitive_array
                .maybe_null_slice::<$P>()
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let vec_validity = primitive_array
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer()
                .iter()
                .collect::<Vec<_>>();
            PrimitiveArray::from_vec(
                indices.iter().map(|i| vec_values[*i]).collect(),
                Validity::from(indices.iter().map(|i| vec_validity[*i]).collect::<Vec<_>>())
            )
            .into_array()
        }),
        DType::Utf8(_) | DType::Binary(_) => {
            let utf8 = array.clone().into_varbin().unwrap();
            let values = utf8
                .with_iterator(|iter| iter.map(|v| v.map(|u| u.to_vec())).collect::<Vec<_>>())
                .unwrap();
            VarBinArray::from_iter(
                indices.iter().map(|i| values[*i].clone()),
                array.dtype().clone(),
            )
            .into_array()
        }
        _ => unreachable!("Array::arbitrary will not generate other dtypes"),
    }
}

pub struct SearchNullableSlice<T>(Vec<Option<T>>);

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

fn search_values_in_primitive_array(
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

pub fn sort_canonical_array(array: &Array) -> Array {
    match array.dtype() {
        DType::Bool(_) => {
            let bool_array = array.clone().into_bool().unwrap();
            let mut opt_values = bool_array
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
            sort_opt_slice(&mut opt_values);
            BoolArray::from_iter(opt_values).into_array()
        }
        DType::Primitive(p, _) => {
            let primitive_array = array.clone().into_primitive().unwrap();
            if p.is_int() {
                match_each_integer_ptype!(p, |$P| {
                    let mut opt_values = primitive_array
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
                        .map(|(p, v)| v.then_some(p))
                        .collect::<Vec<_>>();
                    sort_opt_slice(&mut opt_values);
                    PrimitiveArray::from_nullable_vec(opt_values).into_array()
                })
            } else {
                match_each_float_ptype!(p, |$F| {
                    let mut opt_values = primitive_array
                        .maybe_null_slice::<$F>()
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
                        .map(|(p, v)| v.then_some(p))
                        .collect::<Vec<_>>();
                    opt_values.sort_by(|a, b| match (a, b) {
                        (Some(v), Some(w)) => v.to_bits().cmp(&w.to_bits()),
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => Ordering::Greater,
                        (Some(_), None) => Ordering::Less,
                    });
                    PrimitiveArray::from_nullable_vec(opt_values).into_array()
                })
            }
        }
        DType::Utf8(_) | DType::Binary(_) => {
            let utf8 = array.clone().into_varbin().unwrap();
            let mut opt_values = utf8
                .with_iterator(|iter| iter.map(|v| v.map(|u| u.to_vec())).collect::<Vec<_>>())
                .unwrap();
            sort_opt_slice(&mut opt_values);
            VarBinArray::from_iter(opt_values, array.dtype().clone()).into_array()
        }
        _ => unreachable!("Not a canonical array"),
    }
}

fn sort_opt_slice<T: Ord>(s: &mut [Option<T>]) {
    s.sort_by(|a, b| match (a, b) {
        (Some(v), Some(w)) => v.cmp(w),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
    });
}
