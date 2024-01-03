mod mutable;

use super::Array;
use crate::types::PrimitiveType;
use crate::types::{DType, PType};

#[derive(Clone)]
pub struct PrimitiveArray<T: PrimitiveType> {
    buffer: arrow2::array::PrimitiveArray<T::ArrowType>,
    ptype: PType,
}

impl<T: PrimitiveType> Array for PrimitiveArray<T> {
    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn datatype(&self) -> DType {
        self.ptype.into()
    }
}
