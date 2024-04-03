use crate::array::Array;

#[derive(Debug, Clone)]
pub enum ValidityView<'a> {
    Valid(usize),
    Invalid(usize),
    Array(&'a dyn Array),
}
