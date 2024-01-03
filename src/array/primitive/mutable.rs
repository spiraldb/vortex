use crate::types::{PType, PrimitiveType};

pub struct MutablePrimitiveArray<T: PrimitiveType> {
    buffer: arrow2::array::MutablePrimitiveArray<T::ArrowType>,
    ptype: PType,
}
