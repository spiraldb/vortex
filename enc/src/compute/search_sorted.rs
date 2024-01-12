use arrow2::scalar::Scalar;

use crate::array::Array;
use crate::error::EncResult;

pub enum SearchSortedSide {
    Left,
    Right,
}

pub fn search_sorted_scalar(_values: Array, _n: &dyn Scalar) -> EncResult<&dyn Scalar> {
    todo!()
}
