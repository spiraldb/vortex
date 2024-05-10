use std::future::Future;

pub trait LendingStream {
    type Item<'next>
    where
        Self: 'next;

    fn next(&mut self) -> impl Future<Output = Option<Self::Item<'_>>>;
}
