mod mutable;

use crate::array::{impl_array, Array, ArrayKind};
use crate::types::PrimitiveType;
use crate::types::{DType, PType};

#[derive(Clone)]
pub struct PrimitiveArray<T: PrimitiveType> {
    buffer: arrow2::array::PrimitiveArray<T::ArrowType>,
    ptype: PType,
    dtype: DType,
}

impl<T: PrimitiveType> PrimitiveArray<T> {}

impl<T: PrimitiveType> Array for PrimitiveArray<T> {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn datatype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn kind(&self) -> Option<ArrayKind> {
        Some(ArrayKind::Primitive)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn from_arrow() {}
}
