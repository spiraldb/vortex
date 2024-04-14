use vortex_error::VortexResult;

pub trait ArrayAccessor {
    type Item<'a>;

    fn with_iterator<F: for<'a> FnOnce(&mut dyn Iterator<Item = Self::Item<'a>>) -> R, R>(
        &self,
        f: F,
    ) -> VortexResult<R>;
}
