use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::validity::Validity;
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::flatten_bool;
use crate::compute::scalar_at::scalar_at;
use crate::compute::take::take;
use crate::compute::ArrayCompute;
use crate::serde::ArrayView;
use crate::stats::Stat;

#[derive(Debug, Clone)]
pub enum ValidityView<'a> {
    Valid(usize),
    Invalid(usize),
    Array(&'a dyn Array),
}

impl ValidityView<'_> {
    pub fn to_validity(&self) -> Validity {
        match self {
            Self::Valid(len) => Validity::Valid(*len),
            Self::Invalid(len) => Validity::Invalid(*len),
            Self::Array(a) => Validity::Array(a.to_array()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Valid(len) | Self::Invalid(len) => *len,
            Self::Array(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Valid(len) | Self::Invalid(len) => *len == 0,
            Self::Array(a) => a.is_empty(),
        }
    }

    pub fn all_valid(&self) -> bool {
        match self {
            Self::Valid(_) => true,
            Self::Invalid(_) => false,
            Self::Array(a) => a
                .stats()
                .get_or_compute_as::<usize>(&Stat::TrueCount)
                .map(|true_count| true_count == self.len())
                .unwrap_or(false),
        }
    }

    pub fn all_invalid(&self) -> bool {
        match self {
            Self::Valid(_) => false,
            Self::Invalid(_) => true,
            Self::Array(a) => a
                .stats()
                .get_or_compute_as::<usize>(&Stat::TrueCount)
                .map(|true_count| true_count == 0)
                .unwrap_or(false),
        }
    }

    pub fn to_array(&self) -> ArrayRef {
        match self {
            Self::Valid(len) => ConstantArray::new(true, *len).into_array(),
            Self::Invalid(len) => ConstantArray::new(false, *len).into_array(),
            Self::Array(a) => a.to_array(),
        }
    }

    pub fn to_bool_array(&self) -> BoolArray {
        match self {
            Self::Valid(len) => BoolArray::from(vec![true; *len]),
            Self::Invalid(len) => BoolArray::from(vec![false; *len]),
            Self::Array(a) => flatten_bool(*a).unwrap(),
        }
    }

    pub fn logical_validity(&self) -> Option<Validity> {
        match self.all_valid() {
            true => None,
            false => Some(self.to_validity()),
        }
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        match self {
            Self::Valid(_) => true,
            Self::Invalid(_) => false,
            Self::Array(a) => scalar_at(*a, idx).and_then(|s| s.try_into()).unwrap(),
        }
    }

    pub fn slice(&self, start: usize, stop: usize) -> Validity {
        match self {
            Self::Valid(_) => Validity::Valid(stop - start),
            Self::Invalid(_) => Validity::Invalid(stop - start),
            Self::Array(a) => Validity::Array(Array::slice(*a, start, stop).unwrap()),
        }
    }

    pub fn take(&self, indices: &dyn Array) -> VortexResult<Validity> {
        match self {
            Self::Valid(_) => Ok(Validity::Valid(indices.len())),
            Self::Invalid(_) => Ok(Validity::Invalid(indices.len())),
            Self::Array(a) => Ok(Validity::Array(take(*a, indices)?)),
        }
    }

    pub fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }
}

impl ArrayCompute for ValidityView<'_> {}

impl<'a> From<ArrayView<'a>> for ValidityView<'a> {
    fn from(_value: ArrayView<'a>) -> Self {
        // Parse the metadata, and return the appropriate ValidityView
        todo!()
    }
}
