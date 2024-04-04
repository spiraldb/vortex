use std::sync::Arc;

use arrow_buffer::{BooleanBuffer, NullBuffer};
use itertools::Itertools;
use vortex_error::VortexResult;
use vortex_schema::{DType, Nullability};

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::as_contiguous;
use crate::compute::flatten::flatten_bool;
use crate::compute::scalar_at::scalar_at;
use crate::compute::take::take;
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::stats::{Stat, Stats};
use crate::{impl_array, ArrayWalker};
mod serde;
mod view;

pub use view::*;

use crate::validity::ArrayValidity;

#[derive(Debug, Clone)]
pub enum Validity {
    Valid(usize),
    Invalid(usize),
    Array(ArrayRef),
}

impl Validity {
    pub const DTYPE: DType = DType::Bool(Nullability::NonNullable);

    pub fn array(array: ArrayRef) -> Self {
        if !matches!(array.dtype(), &Validity::DTYPE) {
            panic!("Validity array must be of type bool");
        }
        Self::Array(array)
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
            Validity::Array(a) => a.is_empty(),
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
            Self::Array(a) => a.clone(),
        }
    }

    pub fn to_bool_array(&self) -> BoolArray {
        match self {
            Self::Valid(len) => BoolArray::from(vec![true; *len]),
            Self::Invalid(len) => BoolArray::from(vec![false; *len]),
            Self::Array(a) => flatten_bool(a).unwrap(),
        }
    }

    pub fn logical_validity(&self) -> Validity {
        if self.all_valid() {
            return Validity::Valid(self.len());
        }
        if self.all_invalid() {
            return Validity::Invalid(self.len());
        }
        self.clone()
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        match self {
            Validity::Valid(_) => true,
            Validity::Invalid(_) => false,
            Validity::Array(a) => scalar_at(a, idx).and_then(|s| s.try_into()).unwrap(),
        }
    }

    // TODO(ngates): maybe we want to impl Array for Validity?
    pub fn slice(&self, start: usize, stop: usize) -> Self {
        match self {
            Self::Valid(_) => Self::Valid(stop - start),
            Self::Invalid(_) => Self::Invalid(stop - start),
            Self::Array(a) => Self::Array(a.slice(start, stop).unwrap()),
        }
    }

    pub fn take(&self, indices: &dyn Array) -> VortexResult<Validity> {
        match self {
            Self::Valid(_) => Ok(Self::Valid(indices.len())),
            Self::Invalid(_) => Ok(Self::Invalid(indices.len())),
            Self::Array(a) => Ok(Self::Array(take(a, indices)?)),
        }
    }

    pub fn nbytes(&self) -> usize {
        match self {
            Self::Valid(_) | Self::Invalid(_) => 4,
            Self::Array(a) => a.nbytes(),
        }
    }
}

impl From<NullBuffer> for Validity {
    fn from(value: NullBuffer) -> Self {
        if value.null_count() == 0 {
            Self::Valid(value.len())
        } else if value.null_count() == value.len() {
            Self::Invalid(value.len())
        } else {
            Self::Array(BoolArray::new(value.into_inner(), None).into_array())
        }
    }
}

impl From<BooleanBuffer> for Validity {
    fn from(value: BooleanBuffer) -> Self {
        if value.iter().all(|v| v) {
            Self::Valid(value.len())
        } else if value.iter().all(|v| !v) {
            Self::Invalid(value.len())
        } else {
            Self::Array(BoolArray::new(value, None).into_array())
        }
    }
}

impl From<Vec<bool>> for Validity {
    fn from(value: Vec<bool>) -> Self {
        if value.iter().all(|v| *v) {
            Self::Valid(value.len())
        } else if value.iter().all(|v| !*v) {
            Self::Invalid(value.len())
        } else {
            Self::Array(BoolArray::from(value).into_array())
        }
    }
}

impl PartialEq<Self> for Validity {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        match (self, other) {
            (Self::Valid(_), Self::Valid(_)) => true,
            (Self::Invalid(_), Self::Invalid(_)) => true,
            _ => {
                // TODO(ngates): use compute to dispatch an all() function.
                self.to_bool_array().buffer() == other.to_bool_array().buffer()
            }
        }
    }
}

impl Eq for Validity {}

impl FromIterator<Validity> for Validity {
    fn from_iter<T: IntoIterator<Item = Validity>>(iter: T) -> Self {
        let validities: Vec<Validity> = iter.into_iter().collect();
        let total_len = validities.iter().map(|v| v.len()).sum();

        // If they're all valid, then return a single validity.
        if validities.iter().all(|v| v.all_valid()) {
            return Self::Valid(total_len);
        }
        // If they're all invalid, then return a single invalidity.
        if validities.iter().all(|v| v.all_invalid()) {
            return Self::Invalid(total_len);
        }

        // Otherwise, map each to a bool array and concatenate them.
        let arrays = validities
            .iter()
            .map(|v| v.to_bool_array().into_array())
            .collect_vec();
        Self::Array(as_contiguous(&arrays).unwrap())
    }
}

impl Array for Validity {
    impl_array!();

    fn len(&self) -> usize {
        match self {
            Validity::Valid(len) | Validity::Invalid(len) => *len,
            Validity::Array(a) => a.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Validity::Valid(len) | Validity::Invalid(len) => *len == 0,
            Validity::Array(a) => a.is_empty(),
        }
    }

    fn dtype(&self) -> &DType {
        &Validity::DTYPE
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(match self {
            Validity::Valid(_) => Validity::Valid(stop - start),
            Validity::Invalid(_) => Validity::Invalid(stop - start),
            Validity::Array(a) => Validity::Array(a.slice(start, stop)?),
        }
        .into_array())
    }

    fn encoding(&self) -> EncodingRef {
        &ValidityEncoding
    }

    fn nbytes(&self) -> usize {
        match self {
            Validity::Valid(_) | Validity::Invalid(_) => 8,
            Validity::Array(a) => a.nbytes(),
        }
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        Ok(())
    }
}

impl ArrayValidity for Validity {
    fn logical_validity(&self) -> Validity {
        // Validity is a non-nullable boolean array.
        Validity::Valid(self.len())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

impl ArrayDisplay for Validity {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        match self {
            Validity::Valid(_) => fmt.property("all", "valid"),
            Validity::Invalid(_) => fmt.property("all", "invalid"),
            Validity::Array(a) => fmt.child("validity", a),
        }
    }
}

impl ArrayCompute for Validity {}

#[derive(Debug)]
struct ValidityEncoding;

impl ValidityEncoding {
    const ID: EncodingId = EncodingId::new("vortex.validity");
}

impl Encoding for ValidityEncoding {
    fn id(&self) -> EncodingId {
        ValidityEncoding::ID
    }
}
