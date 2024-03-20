pub mod dtypes;
pub mod wrappers;

pub trait FromArrowType<T>: Sized {
    fn from_arrow_type(value: T) -> Self;
}
