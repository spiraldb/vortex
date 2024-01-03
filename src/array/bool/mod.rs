mod mutable;

use super::Array;
use crate::types::DType;

#[derive(Clone)]
pub struct BoolArray {
    buffer: arrow2::array::BooleanArray,
}

impl Array for BoolArray {

    #[inline]
    fn len(&self) -> usize {
        return self.buffer.len();
    }

    #[inline]
    fn datatype(&self) -> DType {
        DType::Bool
    }
}
