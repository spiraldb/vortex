use super::{impl_array, Array};
use crate::types::DType;

use arrow2::array::BooleanArray as ArrowBooleanArray;

#[derive(Clone)]
pub struct BoolArray {
    buffer: ArrowBooleanArray,
}

pub const KIND: &str = "enc.bool";

impl BoolArray {
    pub fn new(buffer: ArrowBooleanArray) -> Self {
        Self { buffer }
    }
}

impl Array for BoolArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &DType::Bool
    }

    #[inline]
    fn kind(&self) -> &str {
        KIND
    }
}
