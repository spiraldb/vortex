use std::future::Future;
use nougat::gat;

#[gat]
pub trait FallibleLendingIterator {
    type Error;
    type Item<'next>
    where
        Self: 'next;

    fn next(&mut self) -> impl Future<Output = Result<Option<Self::Item<'_>>, Self::Error>>;
}


pub trait FallibleIterator {
    type Error;
    type Item;

    fn next(&mut self) -> impl Future<Output = Result<Option<Self::Item>, Self::Error>>;
}