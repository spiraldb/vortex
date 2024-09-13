mod search_sorted;
mod slice;
mod sort;
mod take;

use std::fmt::Debug;
use std::iter;
use std::ops::Range;

use libfuzzer_sys::arbitrary::Error::EmptyChoose;
use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
pub use sort::sort_canonical_array;
use vortex::array::PrimitiveArray;
use vortex::compute::unary::scalar_at;
use vortex::compute::{SearchResult, SearchSortedSide};
use vortex::{Array, ArrayDType};
use vortex_sampling_compressor::SamplingCompressor;
use vortex_scalar::arbitrary::random_scalar;
use vortex_scalar::{Scalar, StructScalar};

use crate::search_sorted::search_sorted_canonical_array;
use crate::slice::slice_canonical_array;
use crate::take::take_canonical_array;

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
        let action_count = u.int_in_range(1..=4)?;
        for _ in 0..action_count {
            actions.push(match u.int_in_range(0..=3)? {
                0 => {
                    if actions
                        .last()
                        .map(|(l, _)| matches!(l, Action::Compress(_)))
                        .unwrap_or(false)
                    {
                        return Err(EmptyChoose);
                    }
                    (
                        Action::Compress(u.arbitrary()?),
                        ExpectedValue::Array(current_array.clone()),
                    )
                }
                1 => {
                    let start = u.choose_index(current_array.len())?;
                    let stop = u.int_in_range(start..=current_array.len())?;
                    current_array = slice_canonical_array(&current_array, start, stop);

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
                    current_array = take_canonical_array(&current_array, &indices);
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

                    if scalar_contains_null_fields(&scalar) {
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
                        ExpectedValue::Search(search_sorted_canonical_array(
                            &sorted, &scalar, side, None,
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

fn scalar_contains_null_fields(scalar: &Scalar) -> bool {
    if scalar.is_null() {
        return true;
    }

    if let Ok(st) = StructScalar::try_from(scalar) {
        (0..st.fields().unwrap().len())
            .filter_map(|i| st.field_by_idx(i))
            .any(|s| scalar_contains_null_fields(&s))
    } else {
        false
    }
}
