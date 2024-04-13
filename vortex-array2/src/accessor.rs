pub trait ArrayAccessor<'a, T> {
    fn value(&'a self, index: usize) -> Option<T>;
}
