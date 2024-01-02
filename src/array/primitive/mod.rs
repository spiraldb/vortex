mod mutable;

use crate::array::array::Array;
use crate::types::dtype::DType;
use crate::types::ptype::PrimitiveType;

#[derive(Clone)]
pub struct PrimitiveArray<T: PrimitiveType> {
    buffer: arrow2::array::PrimitiveArray<T::ArrowType>,
    ptype: T,
}

impl<T: PrimitiveType> Array for PrimitiveArray<T> {
    fn len(&self) -> usize {
        todo!()
    }

    fn datatype(&self) -> DType {
        todo!()
    }
}
