use crate::ArrayTrait;

pub trait ArrayAccessor<'a, T>: ArrayTrait {
    fn value(&'a self, index: usize) -> Option<T>;
}
