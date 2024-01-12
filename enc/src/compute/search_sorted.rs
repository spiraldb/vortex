use crate::array::Array;
use crate::error::EncResult;
use crate::scalar::Scalar;

pub enum SearchSortedSide {
    Left,
    Right,
}

pub fn search_sorted_scalar(_values: Array, _n: &dyn Scalar) -> EncResult<&dyn Scalar> {
    todo!()
}
