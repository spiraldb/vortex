use nougat::gat;

#[gat]
pub trait FallibleLendingIterator {
    type Error;
    type Item<'next>
    where
        Self: 'next;

    fn next(&mut self) -> Result<Option<Self::Item<'_>>, Self::Error>;
}
