use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, ArrowPrimitiveType, PrimitiveArray as ArrowPrimitiveArray,
};
use arrow_buffer::ScalarBuffer;
use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::arrow::wrappers::as_nulls_view;
use crate::compute::as_arrow::AsArrowArray;
use crate::ptype::PType;
use crate::ptype::{AsArrowPrimitiveType, NativePType};
use crate::validity::ArrayValidity;

impl<T: NativePType + AsArrowPrimitiveType> AsArrowArray for &dyn PrimitiveTrait<T> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        Ok(Arc::new(
            ArrowPrimitiveArray::<<T as AsArrowPrimitiveType>::ArrowType>::new(
                ScalarBuffer::<
                    <<T as AsArrowPrimitiveType>::ArrowType as ArrowPrimitiveType>::Native,
                >::new(self.buffer().clone(), 0, self.len()),
                as_nulls_view(self.validity_view())?,
            ),
        ))
    }
}
