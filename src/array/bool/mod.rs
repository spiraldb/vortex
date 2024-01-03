mod mutable;

use super::{impl_array, Array, ArrayKind};
use crate::types::DType;

#[derive(Clone)]
pub struct BoolArray {
    buffer: arrow2::array::BooleanArray,
}

impl Array for BoolArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn datatype(&self) -> &DType {
        &DType::Bool
    }

    fn kind(&self) -> Option<ArrayKind> {
        Some(ArrayKind::Bool)
    }
}
