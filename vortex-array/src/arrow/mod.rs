mod array;
mod dtypes;
mod recordbatch;
pub mod wrappers;

pub trait FromArrowArray<A> {
    fn from_arrow(array: A, nullable: bool) -> Self;
}

pub trait FromArrowType<T>: Sized {
    fn from_arrow(value: T) -> Self;
}
