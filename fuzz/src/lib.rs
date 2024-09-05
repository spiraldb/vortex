use std::fmt::Debug;
use std::iter;
use std::ops::Range;

use libfuzzer_sys::arbitrary::Error::EmptyChoose;
use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
use vortex::array::PrimitiveArray;
use vortex::compute::unary::scalar_at;
use vortex::compute::SearchSortedSide;
use vortex::{Array, ArrayDType};
use vortex_sampling_compressor::SamplingCompressor;
use vortex_scalar::arbitrary::random_scalar;
use vortex_scalar::Scalar;

#[derive(Debug)]
pub struct FuzzArrayAction {
    pub array: Array,
    pub actions: Vec<Action>,
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
        let len = array.len();
        let action = match u.int_in_range(0..=3)? {
            0 => Action::Compress(u.arbitrary()?),
            1 => {
                let start = u.choose_index(len)?;
                let stop = u.int_in_range(start..=len)?;
                Action::Slice(start..stop)
            }
            2 => {
                if len == 0 {
                    return Err(EmptyChoose);
                }

                let indices = PrimitiveArray::from(random_vec_in_range(u, 0, len - 1)?).into();
                let compressed = SamplingCompressor::default()
                    .compress(&indices, None)
                    .unwrap();
                Action::Take(compressed.into_array())
            }
            3 => {
                let side = if u.arbitrary()? {
                    SearchSortedSide::Left
                } else {
                    SearchSortedSide::Right
                };
                if u.arbitrary()? {
                    let random_value_in_array = scalar_at(&array, u.choose_index(len)?).unwrap();
                    Action::SearchSorted(random_value_in_array, side)
                } else {
                    Action::SearchSorted(random_scalar(u, array.dtype())?, side)
                }
            }
            _ => unreachable!(),
        };

        Ok(Self {
            array,
            actions: vec![action],
        })
    }
}

fn random_vec_in_range(u: &mut Unstructured<'_>, min: usize, max: usize) -> Result<Vec<u64>> {
    iter::from_fn(|| {
        if u.arbitrary().unwrap_or(false) {
            Some(u.int_in_range(min..=max).map(|i| i as u64))
        } else {
            None
        }
    })
    .collect::<Result<Vec<_>>>()
}
