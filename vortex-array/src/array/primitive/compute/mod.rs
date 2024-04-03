use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::primitive::PrimitiveArray;
use crate::array::validity::{Validity, ValidityView};
use crate::array::{Array, ArrayRef};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::cast::CastFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::FlattenFn;
use crate::compute::patch::PatchFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::search_sorted::SearchSortedFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::{AsArrowPrimitiveType, NativePType, PType};
use crate::stats::Stats;
use crate::ArrayWalker;

mod as_arrow;
mod as_contiguous;
mod cast;
mod fill;
mod flatten;
mod patch;
mod scalar_at;
mod search_sorted;
mod take;

pub(crate) trait PrimitiveTrait<T: NativePType>: Debug + Send + Sync {
    fn dtype(&self) -> &DType;

    fn ptype(&self) -> PType;

    fn validity_view(&self) -> Option<ValidityView>;

    fn buffer(&self) -> &Buffer;

    fn to_primitive(&self) -> PrimitiveArray;

    fn typed_data(&self) -> &[T] {
        self.buffer().typed_data::<T>()
    }
}

impl<T: NativePType + AsArrowPrimitiveType> ArrayCompute for &dyn PrimitiveTrait<T> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn cast(&self) -> Option<&dyn CastFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

    fn patch(&self) -> Option<&dyn PatchFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn search_sorted(&self) -> Option<&dyn SearchSortedFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl<T: NativePType> Array for &dyn PrimitiveTrait<T> {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }

    fn into_array(self) -> ArrayRef {
        todo!()
    }

    fn len(&self) -> usize {
        self.typed_data().len()
    }

    fn is_empty(&self) -> bool {
        self.typed_data().is_empty()
    }

    fn dtype(&self) -> &DType {
        (*self).dtype()
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn validity(&self) -> Option<Validity> {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        todo!()
    }

    fn nbytes(&self) -> usize {
        todo!()
    }

    fn with_compute_mut(
        &self,
        _f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        todo!()
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        todo!()
    }
}

impl<T: NativePType> ArrayDisplay for &dyn PrimitiveTrait<T> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}
