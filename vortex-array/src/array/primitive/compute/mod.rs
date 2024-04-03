use arrow_buffer::Buffer;
use vortex_schema::DType;

use crate::array::primitive::PrimitiveArray;
use crate::array::validity::ValidityView;
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
use crate::ptype::{AsArrowPrimitiveType, NativePType, PType};

mod as_arrow;
mod as_contiguous;
mod cast;
mod fill;
mod flatten;
mod patch;
mod scalar_at;
mod search_sorted;
mod take;

pub(crate) trait PrimitiveTrait<T: NativePType> {
    fn dtype(&self) -> &DType;

    fn ptype(&self) -> PType;

    fn len(&self) -> usize;

    fn validity(&self) -> Option<&ValidityView>;

    fn buffer(&self) -> &Buffer;

    fn to_primitive(&self) -> PrimitiveArray;

    fn to_array(&self) -> ArrayRef {
        self.to_primitive().into_array()
    }

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
