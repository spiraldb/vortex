use std::future::Future;

#[must_use = "streams must be polled"]
pub trait Stream {
    type Item;

    fn next(&mut self) -> impl Future<Output = Option<Self::Item>>;

    /// Returns the bounds on the remaining length of the stream.
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
