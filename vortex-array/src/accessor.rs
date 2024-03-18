use crate::array::Array;

pub trait ArrayAccessor<T>: Array {
    fn value(&self, index: usize) -> Option<T>;
}
