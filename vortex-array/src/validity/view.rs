use std::any::Any;
use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::flatten_bool;
use crate::compute::scalar_at::scalar_at;
use crate::compute::take::take;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, ArrayView, WriteCtx};
use crate::stats::{Stat, Stats};
use crate::validity::owned::Validity;
use crate::validity::{ArrayValidity, ValidityEncoding};
use crate::view::{AsView, ToOwnedView};
use crate::ArrayWalker;

#[derive(Debug, Clone)]
pub enum ValidityView<'a> {
    Valid(usize),
    Invalid(usize),
    Array(&'a dyn Array),
}

impl<'v> AsView<'v, ValidityView<'v>> for Validity {
    fn as_view(&'v self) -> ValidityView<'v> {
        match self {
            Self::Valid(len) => ValidityView::Valid(*len),
            Self::Invalid(len) => ValidityView::Invalid(*len),
            Self::Array(a) => ValidityView::Array(a.as_ref()),
        }
    }
}

impl<'v> ToOwnedView<'v> for ValidityView<'v> {
    type Owned = Validity;

    fn to_owned_view(&self) -> Self::Owned {
        match self {
            Self::Valid(len) => Validity::Valid(*len),
            Self::Invalid(len) => Validity::Invalid(*len),
            Self::Array(a) => Validity::Array(a.to_array()),
        }
    }
}

impl ValidityView<'_> {
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
            Self::Valid(len) => ConstantArray::new(true, *len).to_array_data(),
            Self::Invalid(len) => ConstantArray::new(false, *len).to_array_data(),
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
            false => Some(self.to_owned_view()),
        }
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        match self {
            Self::Valid(_) => true,
            Self::Invalid(_) => false,
            Self::Array(a) => scalar_at(*a, idx).and_then(|s| s.try_into()).unwrap(),
        }
    }

    pub fn slice(&self, start: usize, stop: usize) -> VortexResult<Validity> {
        Ok(match self {
            Self::Valid(_) => Validity::Valid(stop - start),
            Self::Invalid(_) => Validity::Invalid(stop - start),
            Self::Array(a) => Validity::Array(Array::slice(*a, start, stop)?),
        })
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

impl<'a> From<ArrayView<'a>> for ValidityView<'a> {
    fn from(_value: ArrayView<'a>) -> Self {
        // FIXME(ngates): parse the metadata, and return the appropriate ValidityView
        ValidityView::Valid(100)
    }
}

impl Array for ValidityView<'_> {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }

    fn to_array_data(self) -> ArrayRef {
        todo!()
    }

    fn len(&self) -> usize {
        match self {
            ValidityView::Valid(len) | ValidityView::Invalid(len) => *len,
            ValidityView::Array(a) => a.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            ValidityView::Valid(len) | ValidityView::Invalid(len) => *len == 0,
            ValidityView::Array(a) => a.is_empty(),
        }
    }

    fn dtype(&self) -> &DType {
        &Validity::DTYPE
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        Ok(Arc::new(self.slice(start, stop)?))
    }

    fn encoding(&self) -> EncodingRef {
        &ValidityEncoding
    }

    fn nbytes(&self) -> usize {
        match self {
            ValidityView::Valid(_) | ValidityView::Invalid(_) => 8,
            ValidityView::Array(a) => a.nbytes(),
        }
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        Ok(())
    }
}

impl ArrayValidity for ValidityView<'_> {
    fn logical_validity(&self) -> Validity {
        // Validity is a non-nullable boolean array.
        Validity::Valid(self.len())
    }

    fn is_valid(&self, _index: usize) -> bool {
        true
    }
}

impl ArrayDisplay for ValidityView<'_> {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        match self {
            ValidityView::Valid(_) => fmt.property("all", "valid"),
            ValidityView::Invalid(_) => fmt.property("all", "invalid"),
            ValidityView::Array(a) => fmt.child("validity", *a),
        }
    }
}

impl ArrayCompute for ValidityView<'_> {}

impl ArraySerde for ValidityView<'_> {
    fn write(&self, _ctx: &mut WriteCtx) -> VortexResult<()> {
        todo!()
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        // TODO: Implement this
        Ok(None)
    }
}
