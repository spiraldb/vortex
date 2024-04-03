use crate::array::Array;

pub trait ArrayAccessor<'a, T>: Array {
    fn value(&'a self, index: usize) -> Option<T>;
}
