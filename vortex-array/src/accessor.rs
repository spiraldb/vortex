use vortex_error::VortexResult;

pub trait ArrayAccessor<Item: ?Sized> {
    fn with_iterator<F, R>(&self, f: F) -> VortexResult<R>
    where
        F: for<'a> FnOnce(&mut (dyn Iterator<Item = Option<&'a Item>>)) -> R;
}
